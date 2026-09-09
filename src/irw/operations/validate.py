"""Check a table against the IRW format standard, without depositing anything.

A contributor working in Python could not check their own data before handing
it over (issue #27). The issue is filed as "port `irw_validate()` from R", but
there is no such function in the R package: what R ships is
`misc/validate_irw.R`, a deliberately standalone five-check subset for someone
with an R session and a URL. The full validator has been Python all along --
`irw_validate` in the pipeline repository, with severity profiles, a CLI and an
exit code.

So this is a wrapper, not a port, and deliberately a thin one. There is exactly
one implementation of the checks, and a copy of them here would recreate the
fork `irw_validate` was built to end: its own docstring records that the checks
had diverged between the R file and `run_qc`, and its tests parse the R file to
assert the two cannot diverge again. A third copy would have nothing keeping it
honest.

`irw_validate` is not a hard dependency. This package is a read client and most
of its users never deposit anything, so the checker is fetched only by those
who need it.

No `[validate]` extra is declared in pyproject.toml yet, deliberately:
`irw-validate` is not on PyPI until ben-domingue/irw#2135 lands and is
released, and an advertised extra that cannot resolve is worse than none. Once
it is published this becomes a two-line addition and
`pip install irw[validate]` works; until then the ImportError below names the
command, which will be correct either way.
"""

from __future__ import annotations

from typing import Any, Optional, Union

import pandas as pd

_INSTALL_HINT = (
    "irw.validate() needs the `irw-validate` package, which is not installed.\n"
    "\n"
    "    pip install irw-validate\n"
    "\n"
    "It is kept separate on purpose: it is the only part of IRW tooling that "
    "checks data you have not deposited yet, and most users of this package "
    "never deposit anything. It needs pandas and nothing else -- no Redivis "
    "account, no credentials, no network."
)


def _load_validator():
    """Import `irw_validate`, or explain how to get it.

    A bare ImportError here names a module the user has never heard of and
    says nothing about what to do, which is the whole difference between a
    missing optional dependency and a broken install.
    """
    try:
        import irw_validate
    except ImportError as exc:  # pragma: no cover - exercised via monkeypatch
        raise ImportError(_INSTALL_HINT) from exc
    return irw_validate


def validate(
    table: Union[pd.DataFrame, str, "Any"],
    profile: str = "upload",
    label: Optional[str] = None,
) -> Any:
    """
    Check a table against the IRW format standard.

    Nothing is uploaded and nothing is contacted: this reads the table you
    give it and reports what would block a deposit.

    Parameters
    ----------
    table : pandas.DataFrame, str, or pathlib.Path
        A frame in IRW long format, or a path to a file holding one. CSV, TSV
        and plain text need nothing extra; `.Rdata`/`.rda`/`.rds` need
        ``pip install 'irw-validate[rdata]'``.
    profile : str, default 'upload'
        Which severity profile to apply. ``'upload'`` is the deposit gate.
        ``'core'`` is the five-check subset that ``misc/validate_irw.R``
        implements, for agreement with the R script. The remaining profiles
        are pipeline concepts; see ``irw_validate.PROFILES``.
    label : str, optional
        Name to report the table under. Defaults to the file name, or
        ``'<DataFrame>'`` for a frame.

    Returns
    -------
    irw_validate.Report
        With ``.ok``, ``.errors`` and ``.warnings``. Render it with
        ``irw_validate.format_report(report)``.

    Raises
    ------
    ImportError
        If `irw-validate` is not installed, with the command to install it.

    Examples
    --------
    >>> import irw, irw_validate            # doctest: +SKIP
    >>> report = irw.validate("my_table.csv")   # doctest: +SKIP
    >>> report.ok                                # doctest: +SKIP
    False
    >>> print(irw_validate.format_report(report))  # doctest: +SKIP

    Notes
    -----
    Severity is a property of the (check, profile) pair rather than of the
    check alone, so the same finding can be an error at the deposit gate and a
    warning elsewhere. That is `irw_validate`'s design and this function does
    not second-guess it.
    """
    validator = _load_validator()

    profiles = getattr(validator, "PROFILES", None)
    # Asked of the installed package rather than hardcoded, so a profile added
    # there does not need a release here to become reachable.
    if profiles is not None and profile not in profiles:
        raise ValueError(
            f"Unknown profile {profile!r}. Available: {', '.join(profiles)}."
        )

    if isinstance(table, pd.DataFrame):
        return validator.validate_frame(table, label=label or "<DataFrame>",
                                        profile=profile)

    # Anything else is treated as a path. Passing it through rather than
    # testing for str/Path means os.PathLike and pathlib subclasses work,
    # and a genuinely wrong type raises from the validator with the file name
    # in the message.
    if label is None:
        return validator.validate_file(table, profile=profile)
    return validator.validate_file(table, profile=profile, label=label)
