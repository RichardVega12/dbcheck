import pandas as pd
from datetime import datetime
import numpy as np

# Columnas base para mostrar en la validación de errores
COLUMNAS_BASE = [
    "Id_Cita", "Anio", "Mes", "Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Id_Ups",
    "Descripcion_Ups", "Nombre_Establecimiento",
    "Numero_Documento_Paciente", "Nombres Completo Paciente",
    "Fecha_Nacimiento_Paciente", "Genero",
    "Numero_Documento_Personal", "Nombres Completo Personal",
    "Id_Condicion_Establecimiento", "Id_Condicion_Servicio",
    "Edad_Reg", "Mes_Actual_Paciente", "Anio_Actual_Paciente", "Tipo_Diagnostico",
    "Valor_Lab", "Codigo_Item", "id_ups", "Hemoglobina", "Observaciones", "Error"
]

def parse_fecha(fecha_str):
    """
    Convierte string de fecha a datetime, manejando múltiples formatos.
    
    Args:
        fecha_str: String de fecha en varios formatos posibles
    
    Returns:
        datetime o pd.NaT si no se puede parsear
    """
    if pd.isna(fecha_str) or fecha_str == '' or fecha_str == 'None' or fecha_str == 'nan' or fecha_str == 'NaT':
        return pd.NaT
    
    # Si ya es datetime, retornar directamente
    if isinstance(fecha_str, (datetime, pd.Timestamp)):
        return fecha_str
    
    # Convertir a string y limpiar
    fecha_str = str(fecha_str).strip()
    
    # Si está vacío después de limpiar, retornar NaT
    if not fecha_str:
        return pd.NaT
    
    # Intentar diferentes formatos de fecha
    formatos = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y',
        '%Y/%m/%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
        '%d/%m/%y', '%m/%d/%y', '%Y%m%d'
    ]
    
    for formato in formatos:
        try:
            return datetime.strptime(fecha_str, formato)
        except ValueError:
            continue
    
    # Si ninguno funciona, retornar NaT
    return pd.NaT

def calcular_edad_formato(fecha_nac, fecha_atencion):
    """
    Calcula la edad en formato Años-Meses-Días.
    
    Args:
        fecha_nac: Fecha de nacimiento (string, datetime o pd.NaT)
        fecha_atencion: Fecha de atención (string, datetime o pd.NaT)
    
    Returns:
        str: Edad formateada (ej: "25A-6M-15D") o string vacío si no se puede calcular
    """
    try:
        # Parsear fechas si son strings
        if isinstance(fecha_nac, str):
            fecha_nac_dt = parse_fecha(fecha_nac)
        else:
            fecha_nac_dt = fecha_nac
            
        if isinstance(fecha_atencion, str):
            fecha_atencion_dt = parse_fecha(fecha_atencion)
        else:
            fecha_atencion_dt = fecha_atencion
        
        # Verificar si alguna fecha es NaT o None
        if pd.isna(fecha_nac_dt) or pd.isna(fecha_atencion_dt):
            return ""
        
        # Calcular diferencia
        if fecha_atencion_dt < fecha_nac_dt:
            return ""  # Fecha de atención anterior a fecha de nacimiento
        
        delta = fecha_atencion_dt - fecha_nac_dt
        total_dias = delta.days
        
        años = total_dias // 365
        meses = (total_dias % 365) // 30
        dias = (total_dias % 365) % 30
        
        return f"{años}A-{meses}M-{dias}D"
    
    except Exception as e:
        print(f"Error calculando edad: {e}")
        return ""

def formatear_fechas(df):
    """
    Formatea las fechas para mostrar en la interfaz y asegura que sean datetime para cálculos.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas de fecha
    
    Returns:
        pd.DataFrame: DataFrame con fechas procesadas
    """
    # Columnas de fecha que necesitan procesamiento
    columnas_fecha = ["Fecha_Atencion", "Fecha_Nacimiento_Paciente", "Fecha_Nacimiento_Personal", 
                     "Fecha_Nacimiento_Registrador", "Fecha_Ultima_Regla", "Fecha_Solicitud_Hb",
                     "Fecha_Resultado_Hb", "Fecha_Registro", "Fecha_Modificacion"]
    
    for col in columnas_fecha:
        if col in df.columns:
            # Primero asegurarnos de que la columna existe y tiene valores
            if df[col].isna().all():
                # Si toda la columna es NaN, crear una columna vacía formateada
                df[col] = ''
            else:
                # Convertir a datetime usando parse_fecha
                df[col + '_dt'] = df[col].apply(parse_fecha)
                
                # Formatear para visualización, manejando NaT
                df[col] = df[col + '_dt'].apply(
                    lambda x: x.strftime('%d/%m/%Y') if not pd.isna(x) else ''
                )
    
    return df

def procesar_dataframe(df):
    """
    Procesa el DataFrame agregando campos calculados para la visualización.
    
    Args:
        df (pd.DataFrame): DataFrame original
    
    Returns:
        pd.DataFrame: DataFrame procesado con campos adicionales
    """
    # Crear nombres completos
    df["Nombres Completo Paciente"] = (
        df.get("Apellido_Paterno_Paciente", pd.Series(index=df.index, dtype=str)).fillna('') + " " +
        df.get("Apellido_Materno_Paciente", pd.Series(index=df.index, dtype=str)).fillna('') + " " +
        df.get("Nombres_Paciente", pd.Series(index=df.index, dtype=str)).fillna('')
    ).str.strip()
    
    df["Nombres Completo Personal"] = (
        df.get("Apellido_Paterno_Personal", pd.Series(index=df.index, dtype=str)).fillna('') + " " +
        df.get("Apellido_Materno_Personal", pd.Series(index=df.index, dtype=str)).fillna('') + " " +
        df.get("Nombres_Personal", pd.Series(index=df.index, dtype=str)).fillna('')
    ).str.strip()
    
    # Asegurar que tenemos las columnas de fecha como datetime para el cálculo
    # Primero procesar las fechas si no se han procesado
    if 'Fecha_Nacimiento_Paciente_dt' not in df.columns and 'Fecha_Nacimiento_Paciente' in df.columns:
        df['Fecha_Nacimiento_Paciente_dt'] = df['Fecha_Nacimiento_Paciente'].apply(parse_fecha)
    
    if 'Fecha_Atencion_dt' not in df.columns and 'Fecha_Atencion' in df.columns:
        df['Fecha_Atencion_dt'] = df['Fecha_Atencion'].apply(parse_fecha)
    
    # Calcular edad usando las columnas datetime
    df["Edad_Reg"] = df.apply(
        lambda row: calcular_edad_formato(
            row.get("Fecha_Nacimiento_Paciente_dt", pd.NaT), 
            row.get("Fecha_Atencion_dt", pd.NaT)
        ), axis=1
    )
    
    # Limpiar columnas temporales si existen
    columnas_a_eliminar = [col for col in df.columns if col.endswith('_dt')]
    if columnas_a_eliminar:
        df = df.drop(columns=columnas_a_eliminar)
    
    return df

def preparar_datos_para_frontend(df, max_filas=100):
    """
    Prepara los datos para enviar al frontend en formato JSON.
    
    Args:
        df (pd.DataFrame): DataFrame a preparar
        max_filas (int): Número máximo de filas a enviar
    
    Returns:
        dict: Datos estructurados para el frontend
    """
    # Seleccionar columnas disponibles
    cols = [c for c in COLUMNAS_BASE if c in df.columns]
    
    # Limitar número de filas
    df_muestra = df[cols].head(max_filas)
    
    # Convertir todas las columnas a string y manejar NaN/NaT
    for col in df_muestra.columns:
        df_muestra[col] = df_muestra[col].astype(str).replace(['nan', 'NaT', 'None'], '', regex=False)
    
    # Estructurar datos para JSON
    data = {
        'columns': cols,
        'data': df_muestra.values.tolist(),
        'total_records': len(df),
        'shown_records': len(df_muestra)
    }
    
    return data