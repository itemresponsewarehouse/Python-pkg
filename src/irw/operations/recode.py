"""Swap identifier columns for short ASCII codes, and put the originals back.

Ports `irw_recode()` and `irw_decode()` from `Rpkg/R/recode.R` (issue #59).
IRW identifiers are whatever the source used -- scripts other than Latin,
accented names, long strings -- which makes subsetting, model formulas and
wide-format column names awkward to type. `recode()` replaces them with codes
such as `P0001` and `I0001`; `decode()` reverses it.

The one design change from R is where the mapping lives. R attaches it to the
data frame as an attribute and reads it back with `irw_recode_key()`, then warns
that most operations drop attributes. pandas has the same hazard --
`DataFrame.attrs` does not survive most operations -- so the key is returned
alongside the frame instead, and `decode()` takes it explicitly. There is no
`recode_key()`: with the key in hand, it has nothing to do. That also removes
R's failure mode where decoding a frame that has quietly lost its attribute
cannot do anything useful.

Codes are assigned in sorted order of each column's unique non-missing values.
R goes out of its way to make that order locale-independent (`method =
"radix"` on `enc2utf8()` strings); Python compares strings by code point
already, so the same order comes free. The tests assert it rather than assume
it.
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

KEY_COLUMNS = ["column", "original", "code"]


def _default_prefix(col: str) -> str:
    """'id' -> P (person), 'item' -> I, anything else -> its first letter."""
    if col == "id":
        return "P"
    if col == "item":
        return "I"
    return col[:1].upper()


def _as_column_list(cols: Union[str, Sequence[str]], name: str) -> List[str]:
    """Accept one column name or a sequence of them."""
    out = [cols] if isinstance(cols, str) else list(cols)
    if not out or not all(isinstance(c, str) for c in out):
        raise ValueError(f"'{name}' must be a column name or a non-empty list of them.")
    return out


def _sort_key(value: Any) -> Tuple[str, str]:
    """Code-point order of the string form, which is what R's radix sort gives.

    Sorting on `str(value)` rather than the values themselves means integer ids
    order the way R orders them ("10" before "2") and a column mixing numbers
    and strings still sorts. The type name only breaks the tie between values
    that print alike, such as 1 and "1", so the order never depends on which
    one happened to appear first.
    """
    return (str(value), type(value).__name__)


def recode(
    df: pd.DataFrame,
    cols: Union[str, Sequence[str]] = ("id", "item"),
    prefix: Optional[Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Recode IRW identifier columns to simple sequential codes.

    Replaces the values of identifier columns (by default ``id`` and ``item``)
    with short ASCII codes such as ``P0001`` and ``I0001``. Useful when
    identifiers are in another script or use characters that are awkward to
    type on a US keyboard, which makes subsetting, model formulas and
    wide-format column names painful.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame in IRW long format.
    cols : str or sequence of str, default ("id", "item")
        Columns to recode.
    prefix : dict of str to str, optional
        Overrides the code prefix per column, e.g. ``{"item": "Q"}``. Defaults
        are ``"P"`` for ``id``, ``"I"`` for ``item``, and the uppercased first
        letter of the column name otherwise.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        A copy of ``df`` with the requested columns replaced by string codes,
        and the key: one row per recoded value, with columns ``column``,
        ``original`` and ``code``. Keep the key -- it is the only record of
        the mapping, and :func:`decode` needs it.

    Warns
    -----
    UserWarning
        For a column with no non-missing values. It is left unchanged and has
        no rows in the key.

    Notes
    -----
    Codes are assigned in code-point order of each column's unique non-missing
    values, so they are deterministic for a given set of values and do not
    depend on locale. Missing values stay missing. Codes are zero-padded to
    ``max(4, len(str(n_unique)))`` digits, so a column with more than 9,999
    values widens rather than collides.

    **Codes are only meaningful relative to their key.** They look canonical,
    but two different subsets of the same table will generally not produce the
    same codes: ``I0003`` in one is not ``I0003`` in the other. Never join or
    compare recoded frames that were not recoded together.

    **Recoding ``item`` breaks joins against** :func:`itemtext`, which is keyed
    on the original item identifiers. Decode first, or join through the key.

    The key is an ordinary DataFrame, so it can be saved alongside the data
    (``key.to_csv(...)``). Keys from separate calls on different columns can be
    combined with ``pandas.concat`` before decoding.

    Examples
    --------
    >>> import pandas as pd
    >>> import irw
    >>> df = pd.DataFrame({
    ...     "id": ["María-01", "María-01", "René-02"],
    ...     "item": ["Q_alpha", "Q_beta", "Q_alpha"],
    ...     "resp": [1, 0, 1],
    ... })
    >>> recoded, key = irw.recode(df)
    >>> recoded["item"].tolist()
    ['I0001', 'I0002', 'I0001']
    >>> irw.decode(recoded, key)["item"].tolist() == df["item"].tolist()
    True
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("'df' must be a pandas DataFrame.")
    cols = _as_column_list(cols, "cols")
    if len(set(cols)) != len(cols):
        # Recoding a column twice would recode its codes, and the key would
        # hold two layers for one column with no way to tell them apart.
        raise ValueError(f"'cols' names a column more than once: {cols}.")
    if prefix is not None:
        if not isinstance(prefix, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in prefix.items()
        ):
            raise ValueError("'prefix' must be a dict of column name to prefix, e.g. {'item': 'Q'}.")

    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required IRW columns: {', '.join(missing_cols)}")

    out = df.copy()
    key_parts = []

    for col in cols:
        # object first, so a categorical column maps its values rather than
        # its categories, and dict lookups see plain Python scalars.
        values = out[col].astype(object)
        uniques = sorted(values[values.notna()].drop_duplicates().tolist(), key=_sort_key)

        if not uniques:
            warnings.warn(
                f"Column '{col}' has no non-missing values; left unchanged.",
                UserWarning,
                stacklevel=2,
            )
            continue

        pfx = (prefix or {}).get(col, _default_prefix(col))
        width = max(4, len(str(len(uniques))))
        codes = [f"{pfx}{i:0{width}d}" for i in range(1, len(uniques) + 1)]

        # Missing values are not keys, so they map to missing.
        out[col] = values.map(dict(zip(uniques, codes)))
        key_parts.append(
            pd.DataFrame(
                {"column": col, "original": pd.Series(uniques, dtype=object), "code": codes}
            )
        )

    if key_parts:
        key = pd.concat(key_parts, ignore_index=True)
    else:
        key = pd.DataFrame({c: pd.Series(dtype=object) for c in KEY_COLUMNS})
    return out, key


def decode(
    df: pd.DataFrame,
    key: pd.DataFrame,
    cols: Optional[Union[str, Sequence[str]]] = None,
) -> pd.DataFrame:
    """
    Restore original identifier values from a :func:`recode` key.

    Works on long-format data, where the columns named in the key are decoded
    in place, and on the wide output of :func:`long2resp`, where item codes
    are column names. A wide frame whose item columns carry an ``item_``
    prefix (as R's ``irw_long2resp()`` writes them) is handled too, and the
    prefix kept.

    Parameters
    ----------
    df : pandas.DataFrame
        A frame returned by :func:`recode`, or derived from one.
    key : pandas.DataFrame
        The key :func:`recode` returned alongside it, with columns ``column``,
        ``original`` and ``code``.
    cols : str or sequence of str, optional
        Columns to decode. Defaults to every column present in both ``df`` and
        the key, plus item codes in the column names when ``df`` has no
        ``item`` column.

    Returns
    -------
    pandas.DataFrame
        A copy of ``df`` with codes replaced by the original values.

    Warns
    -----
    UserWarning
        When values in a decoded column are not in the key. They are left
        unchanged. The usual cause is a key from a different ``recode()``
        call -- codes are only meaningful relative to their own key.

    Raises
    ------
    ValueError
        If ``cols`` names a column ``df`` lacks, or nothing in ``df`` matches
        the key.

    Examples
    --------
    >>> import pandas as pd
    >>> import irw
    >>> df = pd.DataFrame({"id": [1, 1, 2], "item": ["x", "y", "x"], "resp": [1, 0, 1]})
    >>> recoded, key = irw.recode(df)
    >>> wide = irw.long2resp(recoded)  # doctest: +SKIP
    >>> irw.decode(wide, key).columns.tolist()  # doctest: +SKIP
    ['id', 'x', 'y']
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("'df' must be a pandas DataFrame.")
    if not isinstance(key, pd.DataFrame) or not set(KEY_COLUMNS) <= set(key.columns):
        raise ValueError(
            "'key' must be a DataFrame with columns 'column', 'original' and "
            "'code', as returned by recode()."
        )

    auto_cols = cols is None
    if auto_cols:
        cols = [c for c in pd.unique(key["column"]) if c in df.columns]
    else:
        cols = _as_column_list(cols, "cols")
        unknown = [c for c in cols if c not in df.columns]
        if unknown:
            raise ValueError(f"Missing required IRW columns: {', '.join(unknown)}")

    out = df.copy()

    for col in cols:
        k = key[key["column"] == col]
        if k.empty:
            continue
        # str() on the codes, so a key read back from CSV still matches.
        mapping = dict(zip(k["code"].astype(str), k["original"]))
        values = out[col].astype(object)
        matched = values.isin(list(mapping))
        n_unmatched = int((values.notna() & ~matched).sum())
        if n_unmatched:
            warnings.warn(
                f"{n_unmatched} value(s) in '{col}' were not found in the key "
                "and were left unchanged.",
                UserWarning,
                stacklevel=2,
            )
        # infer_objects() hands back the dtype the column had before recoding:
        # integer ids come back as integers, not as object.
        out[col] = values.where(~matched, values.map(mapping)).infer_objects()

    # Wide format: item codes are column names. long2resp() writes them bare;
    # R's irw_long2resp() prefixes them with "item_".
    decoded_names = False
    if auto_cols and "item" not in cols:
        k = key[key["column"] == "item"]
        if not k.empty:
            mapping = dict(zip(k["code"].astype(str), k["original"]))
            names = []
            for name in out.columns:
                bare = name[len("item_"):] if isinstance(name, str) and name.startswith("item_") else name
                if isinstance(bare, str) and bare in mapping:
                    # str(), because long2resp() names columns by the string
                    # form of the item, and a decoded frame should match it.
                    names.append(name[: len(name) - len(bare)] + str(mapping[bare]))
                    decoded_names = True
                else:
                    names.append(name)
            out.columns = names

    if not cols and not decoded_names:
        raise ValueError("Nothing to decode: no columns of 'df' match the key.")

    return out
