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
    keep_columns: Optional[List[str]] = None,
    use_default_columns: bool = True
) -> pd.DataFrame:
    """
    Procesa un DataFrame ya cargado y devuelve un nuevo DF con 'date' y 'hora' (AM/PM) al inicio.
    
    Args:
        keep_columns: Lista de columnas a mantener del DataFrame original (además de 'date', 'hora', 'tag').
                     Si es None, usa _DEFAULT_KEEP_COLUMNS si use_default_columns=True.
        use_default_columns: Si True, usa la lista predefinida cuando keep_columns es None.
                           Si False, mantiene todas las columnas (comportamiento original).
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("process_dataframe espera un pandas.DataFrame válido.")

    col = _find_created_col(df.columns, created_col)
    out = df.copy()
    
# --- Normalización y parseo robusto de fecha/hora ---

# 1) Normaliza 'a.m.'/'p.m.' (variantes) a 'AM'/'PM'
    raw = out[col].astype(str)
    raw = (raw
           .str.replace(r'\s*a[.\s]?m[.]?', ' AM', regex=True, case=False)
           .str.replace(r'\s*p[.\s]?m[.]?', ' PM', regex=True, case=False))

# 2) A datetime (dd/mm/yyyy) con tolerancia
    out[col] = pd.to_datetime(
        raw,
        errors="coerce",
        dayfirst=True,                # <- IMPORTANTE para 21/09/2025
        infer_datetime_format=True
    )

# (si usas tz_from/tz_to, aplica aquí y luego vuelve a dejar naive)
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instálalo para usar conversión de zona horaria.")
    if tz_from and pytz is not None and out[col].notna().any():
        if not hasattr(out[col].dtype, "tz") or out[col].dtype.tz is None:
            out[col] = out[col].dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and out[col].notna().any():
        out[col] = out[col].dt.tz_convert(pytz.timezone(tz_to))
    out[col] = _ensure_naive(out[col])

    # Fecha y hora (12h AM/PM sin cero inicial)
    date_series = out[col].dt.month.astype("Int64").astype(str) + "/" + \
                  out[col].dt.day.astype("Int64").astype(str) + "/" + \
                  out[col].dt.year.astype("Int64").astype(str)
    
    # Hora redondeada hacia abajo (agrupar por hora completa)
    # Ejemplo: 1:30:00 PM -> 1:00:00 PM, 8:07:00 PM -> 8:00:00 PM
      # Hora redondeada a la hora hacia abajo (H:00:00) en formato 12h AM/PM sin cero inicial
      
      
    hora_cerrada = out[col].dt.floor("H")
    hora_series = hora_cerrada.dt.strftime("%I:%M:%S %p").str.lstrip("0")

    
    # Tag como texto: fecha + hora concatenadas
    #tag_text = date_series + "-" + hora_series

    # Tag como número (timestamp en segundos)
    #tag_series = out[col].astype("int64") // 10**9

    # Día: cálculo especial
    # Definimos la hora de corte (5:00 PM = 17:00)
    #HORA_CORTE = 17  # 17:00 es 5pm
    #minutos_corte = 0
    # Obtenemos la hora y minuto del registro
    #horas = out[col].dt.hour
    #minutos = out[col].dt.minute

    # Si la hora es >= 17, el día es el mismo que la fecha
    # Si la hora es < 17, el día es el anterior (fecha menos 1)
    #dias = out[col].dt.date
    #dias = dias.where(horas >= HORA_CORTE, dias - pd.Timedelta(days=1))
    # Convertimos a string con formato M/D/YYYY
    #dia_series = dias.month.astype(str) + "/" + dias.day.astype(str) + "/" + dias.year.astype(str)

    # Insertar las nuevas columnas
    for c in ("hora", "date"):#, "Día"):  "tag", "tag_text"
        if c in out.columns: out = out.drop(columns=[c])
    #out.insert(0, "day", dia_series)
    #out.insert(0, "tag_text", tag_text)
    #out.insert(0, "tag", tag_series)
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    

    if drop_original_created:
        out = out.drop(columns=[col])

    # Selección de columnas: usar keep_columns, o la lista por defecto, o mantener todas
    if keep_columns is not None or use_default_columns:
        # Determinar qué columnas usar
        columns_to_keep = keep_columns if keep_columns is not None else _DEFAULT_KEEP_COLUMNS
        
        # Buscar coincidencias de columns_to_keep en las columnas disponibles (case-insensitive)
        available_cols = list(out.columns)
        cols_lower = {c.lower(): c for c in available_cols}
        
        # Columnas que siempre mantenemos
        final_cols = ["date", "hora", "tag", "tag_text"]
        
        # Agregar las columnas solicitadas que existen
        for requested_col in columns_to_keep:
            # Buscar coincidencia exacta primero
            if requested_col in available_cols:
                if requested_col not in final_cols:
                    final_cols.append(requested_col)
            # Si no encuentra coincidencia exacta, buscar case-insensitive
            elif requested_col.lower() in cols_lower:
                actual_col = cols_lower[requested_col.lower()]
                if actual_col not in final_cols:
                    final_cols.append(actual_col)
            else:
                print(f"[WARN] Columna '{requested_col}' no encontrada en el DataFrame")
        
        # Filtrar el DataFrame solo con las columnas seleccionadas
        out = out[final_cols]
        return out
    else:
        # Comportamiento original: mantener todas las columnas
        ordered = ["date", "hora", "tag", "tag_text"] + [c for c in out.columns if c not in ("date", "hora", "tag", "tag_text")] #, "day"
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
