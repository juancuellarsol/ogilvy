"""
tubular.py
------------------------------
Refactor reutilizable y robusto para exports de Tubular.

Incluye:
- process_file(file_path, ...): lee el archivo y devuelve DF con columnas 'date' y 'hora' (AM/PM)
- process_dataframe(df, ...): procesa un DataFrame ya cargado
- export_df(df, out_path): exporta a .xlsx o .csv
- auto_export(file_path, ...): lee, procesa y guarda en un paso (sufijo y formato)
- batch_export(files, ...): procesa varios archivos

Detalles:
- Hora en formato 12h con AM/PM (sin cero inicial). Ej: '8:00:00 AM' / '8:00:00 PM'
- Fecha en M/D/YYYY (sin ceros a la izquierda)
- Detección flexible de la columna de fecha (por defecto 'Created Time')
- Conversión opcional de zona horaria (tz_from / tz_to) si 'pytz' está disponible

Ejemplos rápidos:
-----------------
# 1) Solo exportar desde archivo
from sprinklr_fechas import auto_export
out = auto_export("XandNewsCO.xlsx", created_col="Created Time", skiprows=2, suffix="_final", fmt="xlsx")

# 2) Revisar primero y luego exportar
from sprinklr_fechas import process_file, export_df
df = process_file("XandNewsCO.xlsx", created_col="Created Time", skiprows=2)
export_df(df, "XandNewsCO_final.xlsx")

# 3) Si ya tienes un DataFrame en memoria
from sprinklr_fechas import process_dataframe
df2 = process_dataframe(df_original, created_col="Created Time")
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional, Sequence, Union, List
import glob as _glob

try:
    import pytz
except Exception:
    pytz = None

_CANDIDATE_CREATED_COLS = [
    "Created Time", "Created time", "created_time", "created time",
    "Fecha de creación", "Fecha", "Date Created", "Timestamp", "Time"
]

# Lista de columnas deseadas por defecto (puedes modificarla aquí)
_DEFAULT_KEEP_COLUMNS = [
    'Date', 'Creator', 'Video_Title', "Video_URL", 
    'Total_Engagements', 'Views', "platform"
]

def _find_created_col(columns: Sequence[str], preferred: Optional[str]) -> str:
    if preferred and preferred in columns:
        return preferred
    for name in _CANDIDATE_CREATED_COLS:
        if name in columns:
            return name
    lowered = {c.lower(): c for c in columns}
    for key, original in lowered.items():
        if any(w in key for w in ("creat", "fecha", "time", "date", "timestamp")):
            return original
    raise KeyError("No pude encontrar la columna de fecha/hora. Especifica 'created_col'.")

def _read_any(file_path: Union[str, Path], skiprows=None, header: Optional[int]=0) -> pd.DataFrame:
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")

def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    # quita zona horaria si viene con tz
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series

def _coerce_datetime(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        s = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
    return s

def process_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = "Created Time",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[Sequence[str]] = None,   # columnas extra a mantener
    use_default_columns: bool = False               # si quieres usar un set por defecto
) -> pd.DataFrame:
    """
    Tubular: crea columnas 'date' (M/D/YYYY) y 'hora' (HH:00:00 AM/PM truncada hacia abajo).
    NO crea 'tag' ni 'tag_text'.
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("process_dataframe espera un pandas.DataFrame válido.")

    col = _find_created_col(df.columns, created_col)
    out = df.copy()

    # 1) Normaliza a.m./p.m. -> AM/PM (variantes)
    raw = out[col].astype(str)
    raw = (raw
           .str.replace(r'\s*a[.\s]?m[.]?', ' AM', regex=True, case=False)
           .str.replace(r'\s*p[.\s]?m[.]?', ' PM', regex=True, case=False))

    # 2) A datetime (dd/mm/yyyy)
    out[col] = pd.to_datetime(
        raw,
        errors="coerce",
        dayfirst=True,        # tus datos vienen como 21/09/2025
        # infer_datetime_format=True  # <- deprecado, no lo uses
    )

    # 3) TZ opcional
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instálalo para usar conversión de zona horaria.")
    if tz_from and pytz is not None and out[col].notna().any():
        if not hasattr(out[col].dtype, "tz") or out[col].dtype.tz is None:
            out[col] = out[col].dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and out[col].notna().any():
        out[col] = out[col].dt.tz_convert(pytz.timezone(tz_to))
    out[col] = _ensure_naive(out[col])

    # 4) Derivadas: date y hora truncada hacia abajo
    date_series = (
        out[col].dt.month.astype("Int64").astype(str) + "/" +
        out[col].dt.day.astype("Int64").astype(str) + "/" +
        out[col].dt.year.astype("Int64").astype(str)
    )
    hour_bucket = out[col].dt.floor("H")
    hora_series = hour_bucket.dt.strftime("%I:00:00 %p").str.lstrip("0")

    # 5) Inserta (elimina si existían)
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)

    # 6) Elimina la columna original si se pide
    if drop_original_created and col in out.columns:
        out = out.drop(columns=[col])

    # 7) Filtrado de columnas final (sin tag/tag_text)
    if use_default_columns and keep_columns is None:
        keep_columns = ["Published_Date", "Platform", "Creator", "Video_Title",
                        "Video_URL", "Total_Engagements", "Views"]

    if keep_columns:
        final_cols = ["date", "hora"] + list(keep_columns)
        out = out[[c for c in final_cols if c in out.columns]]  # <- seguro

    # Orden final
    ordered = ["date", "hora"] + [c for c in out.columns if c not in ("date", "hora")]
    return out[ordered]


def process_file(
    file_path: Union[str, Path],
    created_col: Optional[str] = "Created Time",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[List[str]] = None,
    use_default_columns: bool = True
) -> pd.DataFrame:
    df = _read_any(file_path, skiprows=skiprows, header=header)
    return process_dataframe(
        df, created_col=created_col, tz_from=tz_from, tz_to=tz_to,
        drop_original_created=drop_original_created, keep_columns=keep_columns,
        use_default_columns=use_default_columns
    )

def export_df(df: pd.DataFrame, out_path: Union[str, Path]) -> Path:
    out_path = Path(out_path)
    if out_path.suffix.lower() in (".xlsx", ".xls"):
        df.to_excel(out_path, index=False)
    elif out_path.suffix.lower() == ".csv":
        df.to_csv(out_path, index=False)
    else:
        raise ValueError("Extensión de export no soportada. Usa .xlsx o .csv.")
    return out_path

def _derive_out_path(file_path: Union[str, Path], suffix: str, fmt: str) -> Path:
    file_path = Path(file_path)
    out = file_path.with_stem(file_path.stem + suffix)
    if fmt.lower() == "csv":
        return out.with_suffix(".csv")
    return out.with_suffix(".xlsx")

def auto_export(
    file_path: Union[str, Path],
    created_col: Optional[str] = "Created Time",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[List[str]] = None,
    use_default_columns: bool = True
) -> Path:
    out = _derive_out_path(file_path, suffix, fmt)
    df = process_file(
        file_path=file_path,
        created_col=created_col,
        skiprows=skiprows,
        header=header,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_created=drop_original_created,
        keep_columns=keep_columns,
        use_default_columns=use_default_columns
    )
    export_df(df, out)
    return out

def batch_export(
    files: Sequence[Union[str, Path]],
    created_col: Optional[str] = "Created Time",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[List[str]] = None,
    use_default_columns: bool = True
) -> List[Path]:
    outs: List[Path] = []
    for f in files:
        try:
            out = auto_export(
                f, created_col=created_col, skiprows=skiprows, header=header,
                suffix=suffix, fmt=fmt, tz_from=tz_from, tz_to=tz_to,
                drop_original_created=drop_original_created, keep_columns=keep_columns,
                use_default_columns=use_default_columns
            )
            outs.append(out)
        except Exception as e:
            print(f"[WARN] Falló {f}: {e}")
    return outs

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sprinklr → columnas date/hora (AM/PM), export, batch y DF support.")
    parser.add_argument("--file", help="Ruta a un archivo .xlsx/.csv (modo single).")
    parser.add_argument("--glob", help="Patrón glob para procesar varios archivos (ej. '*.xlsx').")
    parser.add_argument("--created-col", default="Created Time")
    parser.add_argument("--skiprows", type=int, default=None)
    parser.add_argument("--header", type=int, default=0)
    parser.add_argument("--suffix", default="_limpio")
    parser.add_argument("--fmt", default="xlsx", choices=("xlsx", "csv"))
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--tz-from", dest="tz_from", default=None)
    parser.add_argument("--tz-to", dest="tz_to", default=None)
    parser.add_argument("--keep-created", action="store_true")

    args = parser.parse_args()

    targets: List[str] = []
    if args.file:
        targets = [args.file]
    elif args.glob:
        targets = _glob.glob(args.glob)
    else:
        parser.error("Debes pasar --file o --glob.")

    if args.export:
        outs = batch_export(
            targets, created_col=args.created_col, skiprows=args.skiprows, header=args.header,
            suffix=args.suffix, fmt=args.fmt, tz_from=args.tz_from, tz_to=args.tz_to,
            drop_original_created=not args.keep_created
        )
        for o in outs:
            print(f"[OK] Exportado: {o}")
    else:
        df = process_file(
            targets[0], created_col=args.created_col, skiprows=args.skiprows, header=args.header,
            tz_from=args.tz_from, tz_to=args.tz_to, drop_original_created=not args.keep_created
        )
        print(df.head())
