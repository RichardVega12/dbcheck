from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for, session
import pandas as pd
import io
from datetime import datetime
import os
import logging
import traceback
import numpy as np
from werkzeug.utils import secure_filename
import json
import shutil
import zipfile
import tempfile
import duckdb

# Importar módulos separados
from validadores_errores import aplicar_filtro, obtener_funciones_validacion
from funciones_procesamiento import procesar_dataframe, formatear_fechas, preparar_datos_para_frontend
from config_tipos import INTEGER_COLUMNS, DECIMAL_COLUMNS, STRING_COLUMNS, DATE_COLUMNS, FINAL_COLUMNS

app = Flask(__name__, static_folder='static')
app.secret_key = 'your_secret_key_here'

# Directorios
UPLOAD_FOLDER = 'uploads'
DATA_FOLDER = 'data'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# Configuración del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Variable global para almacenar el DataFrame (para validación de errores)
df_global = None

# --- CONFIGURACIÓN PARA CONSOLIDACIÓN ---
APP_CONFIG = {
    'STATIC_MASTERS': {
        'MAESTRO_BUSQUEDA.csv': {'cols': ['ITEM', 'TIPO_BUSQUEDA'], 'rename': {'ITEM': 'Id_Busqueda', 'TIPO_BUSQUEDA': 'Descripcion_Busqueda'}, 'merge_on': 'Id_Busqueda', 'use_custom_load': True},
        'MAESTRO_HIS_PAIS.csv': {'cols': ['Id_Pais', 'Descripcion_Pais'], 'merge_on': 'Id_Pais', 'use_custom_load': True},
        'MAESTRO_HIS_CIE_CPMS.csv': {'cols': ['Codigo_Item', 'Descripcion_Item'], 'merge_on': 'Codigo_Item', 'use_custom_load': True},
        'MAESTRO_HIS_UPS.csv': {'cols': ['Id_Ups', 'Descripcion_Ups'], 'merge_on': 'Id_Ups', 'use_custom_load': True},
        'MAESTRO_HIS_OTRA_CONDicion.csv': {'cols': ['Id_Otra_Condicion', 'Descripcion_Otra_Condicion'], 'merge_on': 'Id_Otra_Condicion'},
        'MAESTRO_HIS_PROFESION.csv': {'cols': ['Id_Profesion', 'Descripcion_Profesion'], 'merge_on': 'Id_Profesion'},
        'MAESTRO_HIS_CONDICION_CONTRATO.csv': {'cols': ['Id_Condicion', 'Descripcion_Condicion'], 'merge_on': 'Id_Condicion'},
        'MAESTRO_HIS_COLEGIO.csv': {'cols': ['Id_Colegio', 'Descripcion_Colegio'], 'merge_on': 'Id_Colegio'},
        'MAESTRO_HIS_ETNIA.csv': {'cols': ['Id_Etnia', 'Descripcion_Etnia'], 'merge_on': 'Id_Etnia'},
        'MAESTRO_HIS_TIPO_DOC.csv': {'cols': ['Id_Tipo_Documento', 'Abrev_Tipo_Doc', 'Descripcion_Tipo_Documento'], 'merge_on': 'Id_Tipo_Documento'},
        'MAESTRO_HIS_FINANCIADOR.csv': {'cols': ['Id_Financiador', 'Descripcion_Financiador'], 'merge_on': 'Id_Financiador'},
        'MAESTRO_HIS_ESTABLECIMIENTO.csv': {
            'cols': ['Id_Establecimiento', 'Nombre_Establecimiento', 'Ubigueo_Establecimiento', 'Codigo_Disa', 'Disa', 'Codigo_Red', 'Red', 'Codigo_MicroRed', 'MicroRed', 'Codigo_Unico', 'Codigo_Sector', 'Descripcion_Sector', 'Departamento', 'Provincia', 'Distrito'],
            'rename': {'Disa': 'Descripcion_Disa', 'Red': 'Descripcion_Red', 'MicroRed': 'Descripcion_MicroRed'},
            'merge_on': 'Id_Establecimiento'
        },
    },
    'DYNAMIC_MASTERS': {
        'NominalTrama': {
            'type': 'plano',
            'merge_on': None,
            'identifier_cols': ['Id_Cita', 'Id_Paciente', 'Id_Establecimiento', 'Codigo_Item', 'Fecha_Atencion'],
            'multiple_files': True
        },
        'MaestroRegistrador': {
            'type': 'registrador',
            'merge_on': 'Id_Registrador',
            'rename_before_merge': {
                'Numero_Documento': 'Numero_Documento_Registrador',
                'Id_Tipo_Documento': 'Id_Tipo_Documento_Registrador',
                'Fecha_Nacimiento': 'Fecha_Nacimiento_Registrador'
            },
            'identifier_cols': ['Id_Registrador', 'Numero_Documento', 'Nombres_Registrador', 'Apellido_Paterno_Registrador'],
            'multiple_files': False
        },
        'MaestroPaciente': {
            'type': 'paciente',
            'merge_on': 'Id_Paciente',
            'rename_before_merge': {
                'Numero_Documento': 'Numero_Documento_Paciente',
                'Id_Tipo_Documento': 'Id_Tipo_Documento_Paciente', 
                'Fecha_Nacimiento': 'Fecha_Nacimiento_Paciente',
                'Domicilio_Declarado': 'Domicilio_declarado',
                'Referencia_Domicilio': 'Referencia_domicilio'
            },
            'identifier_cols': ['Id_Paciente', 'Numero_Documento', 'Nombres_Paciente', 'Genero', 'Apellido_Paterno_Paciente'],
            'multiple_files': False,
            'additional_cols': ['Domicilio_Declarado', 'Referencia_Domicilio']
        },
        'MaestroPersonal': {
            'type': 'personal',
            'merge_on': 'Id_Personal',
            'rename_before_merge': {
                'Numero_Documento': 'Numero_Documento_Personal',
                'Id_Tipo_Documento': 'Id_Tipo_Documento_Personal',
                'Fecha_Nacimiento': 'Fecha_Nacimiento_Personal',
                'Id_Condicion': 'Id_Condicion_Personal',
                'Id_Profesion': 'Id_Profesion_Personal',
                'Id_Colegio': 'Id_Colegio_Personal',
                'Numero_Colegiatura': 'Numero_Colegiatura_Personal'
            },
            'identifier_cols': ['Id_Personal', 'Id_Profesion', 'Nombres_Personal', 'Apellido_Paterno_Personal'],
            'multiple_files': False
        }
    }
}

# --- FUNCIONES DUCKDB ---
def get_duckdb_connection():
    return duckdb.connect()

def pandas_to_duckdb(conn, df, table_name):
    conn.register(table_name, df)
    return table_name

def duckdb_read_csv(conn, filepath, table_name, separator=','):
    try:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{filepath}', 
            delim='{separator}',
            header=true,
            all_varchar=true,
            ignore_errors=true)
        """)
        return True
    except Exception as e:
        app.logger.error(f"Error al leer CSV con DuckDB: {e}")
        return False

# --- FUNCIONES PARA LIMPIAR ARCHIVOS TEMPORALES ---
def limpiar_archivos_temporales():
    try:
        app.logger.info("Iniciando limpieza de archivos temporales...")
        for filename in os.listdir(UPLOAD_FOLDER):
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    app.logger.info(f"Archivo temporal eliminado: {filename}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                app.logger.error(f"Error al eliminar {file_path}: {e}")
        app.logger.info("Limpieza de archivos temporales completada")
    except Exception as e:
        app.logger.error(f"Error al limpiar carpeta uploads: {e}")

def limpiar_archivos_especificos(archivos_a_eliminar):
    if not archivos_a_eliminar:
        return
    app.logger.info(f"Eliminando {len(archivos_a_eliminar)} archivos temporales...")
    for file_path in archivos_a_eliminar:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                app.logger.info(f"Archivo eliminado: {os.path.basename(file_path)}")
            else:
                app.logger.warning(f"Archivo no encontrado: {file_path}")
        except Exception as e:
            app.logger.warning(f"No se pudo eliminar {file_path}: {e}")
    app.logger.info("Eliminación de archivos específicos completada")

# --- FUNCIONES PARA CONSOLIDACIÓN ---
def detect_separator(filepath):
    try:
        with open(filepath, 'r', encoding='latin1') as f:
            first_line = f.readline()
            if ';' in first_line:
                return ';'
            elif ',' in first_line:
                return ','
            elif '\t' in first_line:
                return '\t'
    except Exception as e:
        app.logger.debug(f"DEBUG: Error al detectar separador para {filepath} con latin1: {e}")
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if ';' in first_line:
                    return ';'
                elif ',' in first_line:
                    return ','
                elif '\t' in first_line:
                    return '\t'
        except Exception as e_utf8:
            app.logger.debug(f"DEBUG: Error al detectar separador para {filepath} con utf-8: {e_utf8}")
    return ','

def load_and_preprocess_csv(filepath, expected_cols=None, rename_map=None, fill_zfill=None, use_custom_load=False):
    separator = detect_separator(filepath)
    app.logger.debug(f"DEBUG: Separador detectado para {os.path.basename(filepath)}: '{separator}'")

    df = None
    encoding_used = None

    try:
        if use_custom_load and expected_cols:
            app.logger.debug(f"DEBUG: Usando carga personalizada para {os.path.basename(filepath)}: Nombres = {expected_cols}, usecols = {list(range(len(expected_cols)))}")
            # PRIMERO intentar con UTF-8
            try:
                df = pd.read_csv(filepath, sep=separator, encoding='utf-8', dtype=str, keep_default_na=False, 
                                 engine='python', names=expected_cols, usecols=list(range(len(expected_cols))), header=None)
                encoding_used = 'utf-8'
                app.logger.debug(f"DEBUG: Carga personalizada exitosa con UTF-8: {os.path.basename(filepath)}")
            except UnicodeDecodeError:
                # Si falla UTF-8, intentar con latin1
                df = pd.read_csv(filepath, sep=separator, encoding='latin1', dtype=str, keep_default_na=False, 
                                 engine='python', names=expected_cols, usecols=list(range(len(expected_cols))), header=None)
                encoding_used = 'latin1'
                app.logger.debug(f"DEBUG: Carga personalizada con latin1: {os.path.basename(filepath)}")
        else:
            # PRIMERO intentar con UTF-8 explícitamente
            try:
                df = pd.read_csv(filepath, sep=separator, encoding='utf-8', dtype=str, keep_default_na=False, engine='python')
                encoding_used = 'utf-8'
                app.logger.debug(f"DEBUG: Cargado exitosamente con UTF-8: {os.path.basename(filepath)}")
            except UnicodeDecodeError:
                # Si falla UTF-8, intentar con latin1
                try:
                    df = pd.read_csv(filepath, sep=separator, encoding='latin1', dtype=str, keep_default_na=False, engine='python')
                    encoding_used = 'latin1'
                    app.logger.debug(f"DEBUG: Cargado con latin1: {os.path.basename(filepath)}")
                except Exception as e:
                    app.logger.error(f"Error con latin1 para {os.path.basename(filepath)}: {e}")
                    return None
    except Exception as e:
        app.logger.error(f"Error al cargar {os.path.basename(filepath)}: {e}")
        return None
    
    if df is None:
        app.logger.error(f"No se pudo cargar el archivo {os.path.basename(filepath)} con los encodings probados.")
        return None

    df.columns = df.columns.str.strip()

    if 'ï»¿Id_Cita' in df.columns:
        df.rename(columns={'ï»¿Id_Cita': 'Id_Cita'}, inplace=True)
        app.logger.debug("DEBUG: Columna 'ï»¿Id_Cita' renombrada a 'Id_Cita'.")

    if rename_map:
        original_cols = df.columns.tolist()
        df.rename(columns=rename_map, inplace=True)
        renamed_actual = {old: new for old, new in rename_map.items() if old in original_cols and new in df.columns}
        if renamed_actual:
            app.logger.debug(f"DEBUG: Columnas de {os.path.basename(filepath)} renombradas: {renamed_actual}")

    if fill_zfill and fill_zfill in df.columns:
        df[fill_zfill] = df[fill_zfill].fillna('').astype(str).str.strip().str.zfill(6)
        app.logger.debug(f"DEBUG: Columna '{fill_zfill}' en {os.path.basename(filepath)} formateada con zfill(6).")

    if not use_custom_load and expected_cols:
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ''

    # SOLUCIÓN DEFINITIVA PARA CARACTERES MAL CODIFICADOS
    def fix_encoding(text):
        """Función para corregir encoding de texto"""
        if not isinstance(text, str):
            return text
            
        # Correcciones específicas para caracteres UTF-8 mal interpretados como Latin-1
        encoding_corrections = [
            # Caracteres más comunes en español
            ('Ã¡', 'á'), ('Ã©', 'é'), ('Ã­', 'í'), ('Ã³', 'ó'), ('Ãº', 'ú'),
            ('Ã', 'Á'), ('Ã‰', 'É'), ('Ã', 'Í'), ('Ã“', 'Ó'), ('Ãš', 'Ú'),
            ('Ã±', 'ñ'), ('Ã‘', 'Ñ'), ('Ã¼', 'ü'), ('Ãœ', 'Ü'),
            ('Â¿', '¿'), ('Â¡', '¡'), ('Â°', '°'), ('Âº', 'º'), ('Âª', 'ª'),
            
            # Patrones específicos encontrados en tus datos
            ('aÃ±os', 'años'), ('aÃ±o', 'año'),
            ('CONSEJERÃA', 'CONSEJERÍA'), ('PREVENCIÃ“N', 'PREVENCIÓN'),
            ('RIESGOS EN SALUD MENTAL', 'RIESGOS EN SALUD MENTAL'),
            
            # Caracteres individuales problemáticos
            ('Ã', 'í'), ('Â', ''), ('', ''), ('', ''),
            ('Ã“', 'Ó'), ('Ã“', 'Ó'), ('Ã‰', 'É'),
            
            # Corrección para el caso específico de "Ã±" -> "ñ"
            ('Ã±', 'ñ'), ('Ã‘', 'Ñ'),
            
            # Caracteres de control y espacios raros
            ('\xa0', ' '), ('\r\n', ' '), ('\n', ' '), ('\t', ' '),
        ]
        
        result = text
        for wrong, correct in encoding_corrections:
            result = result.replace(wrong, correct)
            
        return result

    # Aplicar corrección de encoding a todas las columnas de texto
    app.logger.info("Aplicando corrección de encoding a todas las columnas de texto...")
    
    for col in df.columns:
        if col in STRING_COLUMNS or df[col].dtype == 'object':
            # Primero limpieza básica
            df[col] = df[col].astype(str).fillna('').str.strip()
            df[col] = df[col].replace('nan', '', regex=False)
            
            # Eliminar .0 de números enteros
            if df[col].apply(lambda x: isinstance(x, str) and x.endswith('.0')).any():
                df[col] = df[col].apply(lambda x: x.split('.')[0] if isinstance(x, str) and x.endswith('.0') else x)
            
            # Verificar si hay caracteres problemáticos
            has_problematic_chars = df[col].str.contains('Ã|Â||', na=False).any()
            
            if has_problematic_chars:
                # Contar ocurrencias antes de corregir
                count_años = df[col].str.contains('aÃ±os', na=False).sum()
                count_consejeria = df[col].str.contains('CONSEJERÃ', na=False).sum()
                count_prevencion = df[col].str.contains('PREVENCIÃ', na=False).sum()
                
                # Aplicar corrección
                df[col] = df[col].apply(fix_encoding)
                
                # Log de correcciones aplicadas
                if count_años > 0 or count_consejeria > 0 or count_prevencion > 0:
                    app.logger.info(f"✅ Correcciones en '{col}': aÃ±os({count_años}), CONSEJERÃ({count_consejeria}), PREVENCIÃ({count_prevencion})")

    # VERIFICACIÓN FINAL ESPECÍFICA
    app.logger.info("Realizando verificación final de correcciones...")
    
    # Columnas donde esperamos encontrar los problemas
    check_columns = ['Grupo_Edad', 'Descripcion_Item', 'Descripcion_Profesion', 
                    'Descripcion_Condicion', 'Descripcion_Ups', 'Descripcion_Etnia']
    
    for col in check_columns:
        if col in df.columns:
            # Verificar si aún quedan caracteres problemáticos
            still_problematic = df[col].str.contains('aÃ±os|CONSEJERÃ|PREVENCIÃ', na=False).any()
            
            if still_problematic:
                app.logger.warning(f"❌ Aún hay problemas en '{col}'. Aplicando corrección forzada...")
                # Corrección forzada adicional
                df[col] = df[col].str.replace('aÃ±os', 'años', regex=False)
                df[col] = df[col].str.replace('aÃ±o', 'año', regex=False)
                df[col] = df[col].str.replace('CONSEJERÃA', 'CONSEJERÍA', regex=False)
                df[col] = df[col].str.replace('PREVENCIÃ“N', 'PREVENCIÓN', regex=False)
                df[col] = df[col].str.replace('Ã±', 'ñ', regex=False)
                df[col] = df[col].str.replace('Ã“', 'Ó', regex=False)
                df[col] = df[col].str.replace('Ã‰', 'É', regex=False)
            
            # Mostrar muestra de valores corregidos
            sample_values = df[col].dropna().unique()[:2]
            app.logger.debug(f"DEBUG: Valores corregidos en '{col}': {sample_values}")

    app.logger.info(f"{os.path.basename(filepath)} cargado y procesado ({df.shape[0]} filas, {df.shape[1]} columnas) con encoding '{encoding_used}'")
    
    return df

def identify_dynamic_master(filepath):
    basename = os.path.basename(filepath).lower()
    
    if any(keyword in basename for keyword in ['trama', 'plano', 'nominal']):
        app.logger.info(f"Archivo {basename} identificado como: plano (por palabras clave en nombre)")
        return 'plano'
    
    for original_key, config in APP_CONFIG['DYNAMIC_MASTERS'].items():
        if original_key.lower() in basename.lower():
            app.logger.info(f"Archivo {basename} identificado como: {config['type']} (detección inicial por nombre)")
            return config['type']
    
    try:
        # Usar UTF-8 como primera opción también aquí
        try:
            temp_df = pd.read_csv(filepath, sep=detect_separator(filepath), encoding='utf-8', nrows=5, dtype=str, keep_default_na=False, engine='python')
        except UnicodeDecodeError:
            temp_df = pd.read_csv(filepath, sep=detect_separator(filepath), encoding='latin1', nrows=5, dtype=str, keep_default_na=False, engine='python')
            
        temp_df.columns = temp_df.columns.str.strip()
        if 'ï»¿Id_Cita' in temp_df.columns:
            temp_df.rename(columns={'ï»¿Id_Cita': 'Id_Cita'}, inplace=True)
            
        for original_key, config in APP_CONFIG['DYNAMIC_MASTERS'].items():
            identifier_cols = config.get('identifier_cols', [])
            if identifier_cols and all(col in temp_df.columns for col in identifier_cols):
                app.logger.info(f"Archivo {basename} identificado como: {config['type']} (detección por columnas: {identifier_cols})")
                return config['type']
    except Exception as e:
        app.logger.warning(f"No se pudo leer el archivo {basename} para identificar por columnas: {e}")

    app.logger.warning(f"No se pudo identificar el tipo de maestro para el archivo: {basename}")
    return None

def consolidate_multiple_tramas_duckdb(trama_files_paths):
    if not trama_files_paths:
        return None
    
    conn = get_duckdb_connection()
    
    try:
        for i, trama_path in enumerate(trama_files_paths):
            separator = detect_separator(trama_path)
            app.logger.info(f"Procesando trama {i+1}/{len(trama_files_paths)} con DuckDB: {os.path.basename(trama_path)}")
            
            if i == 0:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE consolidated_tramas AS 
                    SELECT * FROM read_csv_auto('{trama_path}', 
                    delim='{separator}',
                    header=true,
                    all_varchar=true,
                    ignore_errors=true)
                """)
                app.logger.info(f"Primera trama cargada con DuckDB. Filas: {conn.execute('SELECT COUNT(*) FROM consolidated_tramas').fetchone()[0]}")
            else:
                conn.execute(f"""
                    INSERT INTO consolidated_tramas 
                    SELECT * FROM read_csv_auto('{trama_path}', 
                    delim='{separator}',
                    header=true,
                    all_varchar=true,
                    ignore_errors=true)
                """)
                app.logger.info(f"Trama adicional agregada con DuckDB. Total acumulado: {conn.execute('SELECT COUNT(*) FROM consolidated_tramas').fetchone()[0]} filas")
        
        result = conn.execute("SELECT * FROM consolidated_tramas").df()
        result.columns = result.columns.str.strip()
        if 'ï»¿Id_Cita' in result.columns:
            result.rename(columns={'ï»¿Id_Cita': 'Id_Cita'}, inplace=True)
            
        app.logger.info(f"Consolidación de tramas con DuckDB completada. Total: {result.shape[0]} filas, {result.shape[1]} columnas")
        return result
        
    except Exception as e:
        app.logger.error(f"Error en consolidación con DuckDB: {e}")
        return None
    finally:
        conn.close()

def consolidate_multiple_tramas(trama_files_paths):
    return consolidate_multiple_tramas_duckdb(trama_files_paths)

def calcular_edades_y_grupo(df):
    """
    Calcula las edades en diferentes unidades y el grupo etario
    """
    app.logger.info("Calculando edades y grupo etario...")
    
    # Asegurarse de que las fechas estén en formato datetime
    if 'Fecha_Nacimiento_Paciente' in df.columns and 'Fecha_Atencion' in df.columns:
        
        # Convertir a datetime si no lo están
        if not pd.api.types.is_datetime64_any_dtype(df['Fecha_Nacimiento_Paciente']):
            df['Fecha_Nacimiento_Paciente'] = pd.to_datetime(df['Fecha_Nacimiento_Paciente'], errors='coerce')
        
        if not pd.api.types.is_datetime64_any_dtype(df['Fecha_Atencion']):
            df['Fecha_Atencion'] = pd.to_datetime(df['Fecha_Atencion'], errors='coerce')
        
        # Fecha actual para cálculos
        fecha_actual = datetime.now()
        
        # Calcular edades en relación a Fecha_Atencion
        mask_fechas_validas = df['Fecha_Nacimiento_Paciente'].notna() & df['Fecha_Atencion'].notna()
        mask_fecha_atencion_mayor = mask_fechas_validas & (df['Fecha_Atencion'] >= df['Fecha_Nacimiento_Paciente'])
        
        # Edades respecto a Fecha_Atencion
        df['Edad_Dias_Paciente_FechaAtencion'] = 0
        df['Edad_Meses_Paciente_FechaAtencion'] = 0
        df['Edad_Anios_Paciente_FechaAtencion'] = 0
        
        df.loc[mask_fecha_atencion_mayor, 'Edad_Dias_Paciente_FechaAtencion'] = (
            df.loc[mask_fecha_atencion_mayor, 'Fecha_Atencion'] - 
            df.loc[mask_fecha_atencion_mayor, 'Fecha_Nacimiento_Paciente']
        ).dt.days
        
        # Calcular meses y años (aproximados)
        df.loc[mask_fecha_atencion_mayor, 'Edad_Anios_Paciente_FechaAtencion'] = (
            (df.loc[mask_fecha_atencion_mayor, 'Fecha_Atencion'].dt.year - 
             df.loc[mask_fecha_atencion_mayor, 'Fecha_Nacimiento_Paciente'].dt.year) -
            ((df.loc[mask_fecha_atencion_mayor, 'Fecha_Atencion'].dt.month - 
              df.loc[mask_fecha_atencion_mayor, 'Fecha_Nacimiento_Paciente'].dt.month) < 0)
        )
        
        df.loc[mask_fecha_atencion_mayor, 'Edad_Meses_Paciente_FechaAtencion'] = (
            df.loc[mask_fecha_atencion_mayor, 'Edad_Anios_Paciente_FechaAtencion'] * 12 +
            (df.loc[mask_fecha_atencion_mayor, 'Fecha_Atencion'].dt.month - 
             df.loc[mask_fecha_atencion_mayor, 'Fecha_Nacimiento_Paciente'].dt.month)
        )
        
        # Edades respecto a Fecha Actual
        mask_fecha_nacimiento_valida = df['Fecha_Nacimiento_Paciente'].notna()
        mask_fecha_actual_mayor = mask_fecha_nacimiento_valida & (pd.Timestamp(fecha_actual) >= df['Fecha_Nacimiento_Paciente'])
        
        df['Edad_Dias_Paciente_FechaActual'] = 0
        df['Edad_Meses_Paciente_FechaActual'] = 0
        df['Edad_Anios_Paciente_FechaActual'] = 0
        
        df.loc[mask_fecha_actual_mayor, 'Edad_Dias_Paciente_FechaActual'] = (
            pd.Timestamp(fecha_actual) - 
            df.loc[mask_fecha_actual_mayor, 'Fecha_Nacimiento_Paciente']
        ).dt.days
        
        # Calcular meses y años actuales (aproximados)
        df.loc[mask_fecha_actual_mayor, 'Edad_Anios_Paciente_FechaActual'] = (
            (fecha_actual.year - 
             df.loc[mask_fecha_actual_mayor, 'Fecha_Nacimiento_Paciente'].dt.year) -
            ((fecha_actual.month - 
              df.loc[mask_fecha_actual_mayor, 'Fecha_Nacimiento_Paciente'].dt.month) < 0)
        )
        
        df.loc[mask_fecha_actual_mayor, 'Edad_Meses_Paciente_FechaActual'] = (
            df.loc[mask_fecha_actual_mayor, 'Edad_Anios_Paciente_FechaActual'] * 12 +
            (fecha_actual.month - 
             df.loc[mask_fecha_actual_mayor, 'Fecha_Nacimiento_Paciente'].dt.month)
        )
        
        app.logger.info(f"Edades calculadas: {mask_fecha_atencion_mayor.sum()} registros con fechas válidas")
    
    # Calcular Grupo_Edad - CORREGIDO
    df['Grupo_Edad'] = ''
    
    if 'Tipo_Edad' in df.columns and 'Edad_Reg' in df.columns:
        # Asegurar que Edad_Reg sea numérico
        if not pd.api.types.is_numeric_dtype(df['Edad_Reg']):
            df['Edad_Reg'] = pd.to_numeric(df['Edad_Reg'], errors='coerce')
        
        # Aplicar lógica de grupos etarios - USAR MÉTODO MÁS SIMPLE
        mask_edad_años = (df['Tipo_Edad'] == 'A') & df['Edad_Reg'].notna()
        
        # Crear una copa del DataFrame para trabajar con las condiciones
        df_temp = df.loc[mask_edad_años].copy()
        
        if not df_temp.empty:
            # Aplicar condiciones directamente
            condiciones = [
                df_temp['Edad_Reg'].between(0, 11),
                df_temp['Edad_Reg'].between(12, 17),
                df_temp['Edad_Reg'].between(18, 29),
                df_temp['Edad_Reg'].between(30, 59),
                df_temp['Edad_Reg'] >= 60
            ]
            
            opciones = [
                '0 a 11 años',
                '12 a 17 años', 
                '18 a 29 años',
                '30 a 59 años',
                '60 años a más'
            ]
            
            # Usar np.select en el DataFrame temporal
            grupo_edad_temp = np.select(condiciones, opciones, default='')
            
            # Asignar los resultados al DataFrame original
            df.loc[mask_edad_años, 'Grupo_Edad'] = grupo_edad_temp
        
        app.logger.info(f"Grupos etarios calculados: {(df['Grupo_Edad'] != '').sum()} registros asignados")
    
    return df

def process_dynamic_masters_pandas(consolidado, all_masters):
    app.logger.info("Pre-procesando maestros dinámicos y uniéndolos con sus estáticos específicos...")

    if 'paciente' in all_masters['dynamic']:
        df_paciente = all_masters['dynamic']['paciente'].copy()
        original_key_paciente = next((k for k, v in APP_CONFIG['DYNAMIC_MASTERS'].items() if v['type'] == 'paciente'), None)
        
        if original_key_paciente: 
            config_paciente = APP_CONFIG['DYNAMIC_MASTERS'][original_key_paciente] 
            
            renamed_cols_paciente = {}
            for original_col, desired_col in config_paciente.get('rename_before_merge', {}).items():
                if original_col in df_paciente.columns:
                    df_paciente.rename(columns={original_col: desired_col}, inplace=True)
                    renamed_cols_paciente[original_col] = desired_col
            if renamed_cols_paciente:
                app.logger.debug(f"DEBUG: Renombres aplicados en maestro 'paciente': {renamed_cols_paciente}")

            # AGREGAR CAMPOS ADICIONALES DEL PACIENTE (sin Domicilio_declarado)
            additional_cols = config_paciente.get('additional_cols', [])
            # Remover Domicilio_declarado si está presente
            if 'Domicilio_declarado' in additional_cols:
                additional_cols.remove('Domicilio_declarado')
            for col in additional_cols:
                if col not in df_paciente.columns:
                    df_paciente[col] = ''
                    app.logger.debug(f"DEBUG: Columna adicional '{col}' agregada a maestro paciente")

            if 'etnia' in all_masters['static'] and 'Id_Etnia' in df_paciente.columns:
                df_paciente = pd.merge(df_paciente, all_masters['static']['etnia'][['Id_Etnia', 'Descripcion_Etnia']],
                                    on='Id_Etnia', how='left')
                app.logger.info("Maestro 'etnia' unido a 'paciente'.")
            
            if 'tipo_doc' in all_masters['static'] and 'Id_Tipo_Documento_Paciente' in df_paciente.columns:
                df_tipo_doc_paciente_temp = all_masters['static']['tipo_doc'][['Id_Tipo_Documento', 'Descripcion_Tipo_Documento', 'Abrev_Tipo_Doc']].copy()
                df_tipo_doc_paciente_temp.rename(columns={
                    'Descripcion_Tipo_Documento': 'Descripcion_Tipo_Documento_Paciente',
                    'Abrev_Tipo_Doc': 'Abrev_Tipo_Doc_Paciente'
                }, inplace=True)
                df_paciente = pd.merge(df_paciente, df_tipo_doc_paciente_temp,
                                    left_on='Id_Tipo_Documento_Paciente', right_on='Id_Tipo_Documento', how='left')
                df_paciente.drop(columns=['Id_Tipo_Documento'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'tipo_doc' unido a 'paciente' para sus descripciones.")
            
            all_masters['dynamic']['paciente'] = df_paciente

    if 'personal' in all_masters['dynamic']:
        df_personal = all_masters['dynamic']['personal'].copy()
        original_key_personal = next((k for k, v in APP_CONFIG['DYNAMIC_MASTERS'].items() if v['type'] == 'personal'), None)
        
        if original_key_personal: 
            config_personal = APP_CONFIG['DYNAMIC_MASTERS'][original_key_personal] 
            
            renamed_cols_personal = {}
            for original_col, desired_col in config_personal.get('rename_before_merge', {}).items():
                if original_col in df_personal.columns:
                    df_personal.rename(columns={original_col: desired_col}, inplace=True)
                    renamed_cols_personal[original_col] = desired_col
            if renamed_cols_personal:
                app.logger.debug(f"DEBUG: Renombres aplicados en maestro 'personal': {renamed_cols_personal}")
            
            if 'condicion_contrato' in all_masters['static'] and 'Id_Condicion_Personal' in df_personal.columns:
                df_personal = pd.merge(df_personal, all_masters['static']['condicion_contrato'][['Id_Condicion', 'Descripcion_Condicion']],
                                    left_on='Id_Condicion_Personal', right_on='Id_Condicion', how='left')
                df_personal.drop(columns=['Id_Condicion'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'condicion_contrato' unido a 'personal'.")

            if 'profesion' in all_masters['static'] and 'Id_Profesion_Personal' in df_personal.columns:
                df_personal = pd.merge(df_personal, all_masters['static']['profesion'][['Id_Profesion', 'Descripcion_Profesion']],
                                    left_on='Id_Profesion_Personal', right_on='Id_Profesion', how='left')
                df_personal.drop(columns=['Id_Profesion'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'profesion' unido a 'personal'.")

            if 'colegio' in all_masters['static'] and 'Id_Colegio_Personal' in df_personal.columns:
                df_personal = pd.merge(df_personal, all_masters['static']['colegio'][['Id_Colegio', 'Descripcion_Colegio']],
                                    left_on='Id_Colegio_Personal', right_on='Id_Colegio', how='left')
                df_personal.drop(columns=['Id_Colegio'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'colegio' unido a 'personal'.")

            if 'tipo_doc' in all_masters['static'] and 'Id_Tipo_Documento_Personal' in df_personal.columns:
                df_tipo_doc_personal_temp = all_masters['static']['tipo_doc'][['Id_Tipo_Documento', 'Descripcion_Tipo_Documento', 'Abrev_Tipo_Doc']].copy()
                df_tipo_doc_personal_temp.rename(columns={
                    'Descripcion_Tipo_Documento': 'Descripcion_Tipo_Documento_Personal',
                    'Abrev_Tipo_Doc': 'Abrev_Tipo_Doc_Personal'
                }, inplace=True)
                df_personal = pd.merge(df_personal, df_tipo_doc_personal_temp,
                                    left_on='Id_Tipo_Documento_Personal', right_on='Id_Tipo_Documento', how='left')
                df_personal.drop(columns=['Id_Tipo_Documento'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'tipo_doc' unido a 'personal' para sus descripciones.")
            
            all_masters['dynamic']['personal'] = df_personal

    if 'registrador' in all_masters['dynamic']:
        df_registrador = all_masters['dynamic']['registrador'].copy()
        original_key_registrador = next((k for k, v in APP_CONFIG['DYNAMIC_MASTERS'].items() if v['type'] == 'registrador'), None)
        
        if original_key_registrador: 
            config_registrador = APP_CONFIG['DYNAMIC_MASTERS'][original_key_registrador]
            
            renamed_cols_registrador = {}
            for original_col, desired_col in config_registrador.get('rename_before_merge', {}).items():
                if original_col in df_registrador.columns:
                    df_registrador.rename(columns={original_col: desired_col}, inplace=True)
                    renamed_cols_registrador[original_col] = desired_col
            if renamed_cols_registrador:
                app.logger.debug(f"DEBUG: Renombres aplicados en maestro 'registrador': {renamed_cols_registrador}")

            if 'tipo_doc' in all_masters['static'] and 'Id_Tipo_Documento_Registrador' in df_registrador.columns:
                df_tipo_doc_registrador_temp = all_masters['static']['tipo_doc'][['Id_Tipo_Documento', 'Descripcion_Tipo_Documento', 'Abrev_Tipo_Doc']].copy()
                df_tipo_doc_registrador_temp.rename(columns={
                    'Descripcion_Tipo_Documento': 'Descripcion_Tipo_Documento_Registrador',
                    'Abrev_Tipo_Doc': 'Abrev_Tipo_Doc_Registrador'
                }, inplace=True)
                df_registrador = pd.merge(df_registrador, df_tipo_doc_registrador_temp,
                                        left_on='Id_Tipo_Documento_Registrador', right_on='Id_Tipo_Documento', how='left')
                df_registrador.drop(columns=['Id_Tipo_Documento'], inplace=True, errors='ignore')
                app.logger.info("Maestro 'tipo_doc' unido a 'registrador' para sus descripciones.")
            
            all_masters['dynamic']['registrador'] = df_registrador

    dynamic_masters_to_merge = {
        'paciente': 'Id_Paciente',
        'personal': 'Id_Personal',
        'registrador': 'Id_Registrador'
    }

    for master_name, merge_col in dynamic_masters_to_merge.items():
        df_dynamic = all_masters['dynamic'].get(master_name)
        if df_dynamic is None or df_dynamic.empty:
            app.logger.warning(f"Maestro dinámico '{master_name}' no disponible o vacío. Saltando unión.")
            continue
        
        if merge_col not in consolidado.columns:
            app.logger.warning(f"No se pudo unir con maestro dinámico '{master_name}'. Columna de unión '{merge_col}' no encontrada en el consolidado.")
            continue
        if merge_col not in df_dynamic.columns:
            app.logger.warning(f"No se pudo unir con maestro dinámico '{master_name}'. Columna de unión '{merge_col}' no encontrada en el maestro dinámico.")
            continue

        consolidado[merge_col] = consolidado[merge_col].astype(str).str.strip()
        df_dynamic[merge_col] = df_dynamic[merge_col].astype(str).str.strip()

        # PARA PACIENTE: INCLUIR CAMPOS ADICIONALES (sin Domicilio_declarado)
        cols_to_keep_from_dynamic = [col for col in df_dynamic.columns if col in FINAL_COLUMNS or col == merge_col]
        if master_name == 'paciente':
            config_paciente = APP_CONFIG['DYNAMIC_MASTERS'].get('MaestroPaciente', {})
            additional_cols = config_paciente.get('additional_cols', [])
            # Remover Domicilio_declarado si está presente
            if 'Domicilio_declarado' in additional_cols:
                additional_cols.remove('Domicilio_declarado')
            cols_to_keep_from_dynamic.extend(additional_cols)
            app.logger.debug(f"DEBUG: Campos adicionales para paciente: {additional_cols}")
        
        if master_name == 'personal':
             cols_to_keep_from_dynamic.extend(['Id_Condicion_Personal', 'Id_Profesion_Personal', 'Id_Colegio_Personal', 'Numero_Colegiatura_Personal'])
        
        df_dynamic_filtered = df_dynamic[list(set(cols_to_keep_from_dynamic))]

        app.logger.info(f"- Uniendo con maestro dinámico '{master_name}' en '{merge_col}'...")
        initial_rows = consolidado.shape[0]
        
        original_consolidado_cols = consolidado.columns.tolist()
        
        consolidado = pd.merge(consolidado, df_dynamic_filtered, on=merge_col, how='left', suffixes=('', f'_{master_name}_drop'))
        
        for col in consolidado.columns:
            if col.endswith(f'_{master_name}_drop'):
                base_col = col.replace(f'_{master_name}_drop', '')
                if base_col in original_consolidado_cols or base_col not in FINAL_COLUMNS:
                    consolidado.drop(columns=[col], inplace=True)
            
        app.logger.info(f"Unión con '{master_name}' completada. Filas antes: {initial_rows}, Filas después: {consolidado.shape[0]}.")

    app.logger.info("Realizando limpieza final, conversión de tipos y selección de columnas...")
    
    column_mapping_for_personal = {
        'Id_Condicion_Personal': 'Id_Condicion',
        'Id_Profesion_Personal': 'Id_Profesion',
        'Id_Colegio_Personal': 'Id_Colegio',
        'Numero_Colegiatura_Personal': 'Numero_Colegiatura'
    }

    for original_col, final_col in column_mapping_for_personal.items():
        if original_col in consolidado.columns:
            if final_col in consolidado.columns:
                consolidado[final_col] = consolidado[final_col].replace('', np.nan)
                consolidado[final_col].fillna(consolidado[original_col], inplace=True)
                consolidado[final_col] = consolidado[final_col].fillna('')
                app.logger.debug(f"DEBUG: Consolidando datos de '{original_col}' en '{final_col}'.")
            else:
                consolidado[final_col] = consolidado[original_col]
                app.logger.debug(f"DEBUG: Copiando '{original_col}' a nueva columna '{final_col}'.")
            consolidado.drop(columns=[original_col], errors='ignore', inplace=True)

    # CALCULAR EDADES Y GRUPO ETARIO
    consolidado = calcular_edades_y_grupo(consolidado)

    # CONVERSIÓN DE TIPOS DE DATOS SEGÚN SQL SERVER
    app.logger.info("Convirtiendo tipos de datos según SQL Server...")
    
    # Campos enteros
    for col in INTEGER_COLUMNS:
        if col in consolidado.columns:
            consolidado[col] = consolidado[col].astype(str).str.strip()
            consolidado[col] = consolidado[col].replace({'nan': '', 'None': '', '<NA>': ''}, regex=False)
            # Convertir a numérico, manejar errores y luego a Int64 (que soporta NaN)
            consolidado[col] = pd.to_numeric(consolidado[col], errors='coerce')
            consolidado[col] = consolidado[col].astype('Int64')  # Int64 permite valores nulos
            app.logger.debug(f"DEBUG: Columna '{col}' convertida a entero.")
        else:
            consolidado[col] = pd.NA  # Usar pd.NA para enteros
            app.logger.warning(f"Columna entera '{col}' no encontrada.")

    # Campos decimales
    for col in DECIMAL_COLUMNS:
        if col in consolidado.columns:
            consolidado[col] = consolidado[col].astype(str).str.strip()
            consolidado[col] = consolidado[col].replace({'nan': '', 'None': '', '<NA>': ''}, regex=False)
            consolidado[col] = pd.to_numeric(consolidado[col], errors='coerce')
            app.logger.debug(f"DEBUG: Columna '{col}' convertida a decimal.")
        else:
            consolidado[col] = None
            app.logger.warning(f"Columna decimal '{col}' no encontrada.")

    # Campos de fecha
    app.logger.info("Preparando columnas de fecha para el formato de Excel...")
    for col in DATE_COLUMNS:
        if col in consolidado.columns:
            consolidado[col] = consolidado[col].astype(str).str.strip()
            consolidado[col] = consolidado[col].replace({'nan': '', 'None': '', '<NA>': ''}, regex=False)
            consolidado[col] = pd.to_datetime(consolidado[col], errors='coerce')
            app.logger.debug(f"DEBUG: Columna '{col}' convertida a datetime.")
        else:
            consolidado[col] = pd.NaT
            app.logger.warning(f"Columna de fecha '{col}' no encontrada.")

    # Lógica específica para Ficha_Familiar
    app.logger.info("Aplicando lógica específica para 'Ficha_Familiar'...")
    
    if 'Ficha_Familiar' in consolidado.columns:
        consolidado['Ficha_Familiar'] = consolidado['Ficha_Familiar'].astype(str).str.strip().replace('nan', '', regex=False)
    else:
        consolidado['Ficha_Familiar'] = ''

    if 'Id_Paciente' in consolidado.columns:
        consolidado['Id_Paciente'] = consolidado['Id_Paciente'].astype(str).str.strip().replace('nan', '', regex=False)
    else:
        consolidado['Id_Paciente'] = ''

    if 'Numero_Documento_Paciente' in consolidado.columns:
        consolidado['Numero_Documento_Paciente'] = consolidado['Numero_Documento_Paciente'].astype(str).str.strip().replace('nan', '', regex=False)
    else:
        consolidado['Numero_Documento_Paciente'] = ''

    condition_ficha_empty = (consolidado['Ficha_Familiar'] == '') | (consolidado['Ficha_Familiar'].isna())
    condition_doc_empty = (consolidado['Numero_Documento_Paciente'] == '') | (consolidado['Numero_Documento_Paciente'].isna())
    condition_id_paciente_not_empty = (consolidado['Id_Paciente'] != '') & (consolidado['Id_Paciente'].notna())

    consolidado.loc[condition_ficha_empty & condition_doc_empty & condition_id_paciente_not_empty, 'Ficha_Familiar'] = consolidado['Id_Paciente']
    
    app.logger.info("Lógica de 'Ficha_Familiar' aplicada.")

    # REINDEX
    app.logger.info(f"Columnas ANTES del reindex: {list(consolidado.columns)}")
    missing_cols = [col for col in FINAL_COLUMNS if col not in consolidado.columns]
    if missing_cols:
        app.logger.warning(f"Columnas FALTANTES: {missing_cols}")
        for col in missing_cols:
            # Asignar tipo correcto según la categoría de la columna
            if col in INTEGER_COLUMNS:
                consolidado[col] = pd.NA
            elif col in DECIMAL_COLUMNS:
                consolidado[col] = None
            elif col in DATE_COLUMNS:
                consolidado[col] = pd.NaT
            else:
                consolidado[col] = ''
    consolidado = consolidado.reindex(columns=FINAL_COLUMNS)
    
    app.logger.info("🎉 CONSOLIDACIÓN FINALIZADA: {} registros, {} columnas".format(consolidado.shape[0], consolidado.shape[1]))
    return consolidado

def generate_consolidated_data_duckdb(uploaded_files_paths):
    conn = get_duckdb_connection()
    
    try:
        all_masters = {'static': {}, 'dynamic': {}}
        
        # Cargar maestros estáticos
        app.logger.info("Cargando maestros estáticos...")
        for filename, config in APP_CONFIG['STATIC_MASTERS'].items():
            master_path = os.path.join(DATA_FOLDER, filename)
            df = load_and_preprocess_csv(
                master_path,
                expected_cols=config.get('cols'),
                rename_map=None,
                fill_zfill='Id_Establecimiento' if filename == 'MAESTRO_HIS_ESTABLECIMIENTO.csv' else None,
                use_custom_load=config.get('use_custom_load', False)
            )
            if df is None:
                flash(f"Error al cargar el archivo estático: {filename}. No se pudo generar el consolidado.", "error")
                return None
            logical_name = os.path.splitext(filename)[0].replace('MAESTRO_HIS_', '').replace('MAESTRO_', '').lower()
            all_masters['static'][logical_name] = df
            
            table_name = f"static_{logical_name}"
            conn.register(table_name, df)
        
        # Identificar y cargar maestros dinámicos
        app.logger.info("Identificando y cargando maestros dinámicos (subidos por el usuario)...")
        
        trama_files = []
        other_dynamic_files = {}
        
        for upload_path in uploaded_files_paths:
            master_type = identify_dynamic_master(upload_path)
            if master_type:
                if master_type == 'plano':
                    trama_files.append(upload_path)
                else:
                    other_dynamic_files[master_type] = upload_path
            else:
                flash(f"No se pudo identificar el tipo de archivo para: {os.path.basename(upload_path)}. Saltando archivo.", "warning")
        
        # Consolidar tramas con DuckDB
        if trama_files:
            app.logger.info(f"Encontrados {len(trama_files)} archivos de trama/plano. Consolidando con DuckDB...")
            plano_df = consolidate_multiple_tramas_duckdb(trama_files)
            if plano_df is None:
                flash("Error al consolidar los archivos de trama/plano.", "error")
                return None
            all_masters['dynamic']['plano'] = plano_df

            if 'Id_Establecimiento' in plano_df.columns:
                plano_df['Id_Establecimiento'] = plano_df['Id_Establecimiento'].fillna('').astype(str).str.strip().str.zfill(6)
                app.logger.info("Id_Establecimiento en trama (plano) formateado con zfill(6) antes del JOIN.")
            else:
                app.logger.warning("Columna 'Id_Establecimiento' NO encontrada en trama.")
                plano_df['Id_Establecimiento'] = ''

            conn.register('plano', plano_df)

            app.logger.info(f"Columnas en trama (plano): {list(plano_df.columns)}")
            sample_ids = plano_df['Id_Establecimiento'].dropna().unique()[:5].tolist()
            app.logger.info(f"Ejemplos de Id_Establecimiento en trama (zfill): {sample_ids}")

        else:
            flash("No se encontraron archivos de trama/plano. Son requeridos para la consolidación.", "error")
            return None
        
        # Cargar otros maestros dinámicos
        for master_type, file_path in other_dynamic_files.items():
            df = load_and_preprocess_csv(file_path)
            if df is None:
                flash(f"Error al cargar el archivo dinámico: {os.path.basename(file_path)}. No se pudo generar el consolidado.", "error")
                return None
            all_masters['dynamic'][master_type] = df
            conn.register(f"dynamic_{master_type}", df)
        
        # Realizar la consolidación principal con DuckDB
        app.logger.info("Iniciando consolidación principal con DuckDB...")
        
        select_columns = ["p.*"]
        join_clauses = []
        
        # Usar alias únicos para cada tabla
        for filename, config in APP_CONFIG['STATIC_MASTERS'].items():
            logical_name = os.path.splitext(filename)[0].replace('MAESTRO_HIS_', '').replace('MAESTRO_', '').lower()
            merge_col = config.get('merge_on')
            
            if merge_col and merge_col in plano_df.columns:
                table_name = f"static_{logical_name}"
                alias = f"s_{logical_name}"  # Alias único para cada tabla
                cols_to_select_temp = []
                
                app.logger.info(f"Procesando maestro estático: {logical_name} | merge_on: {merge_col}")
                
                for col in config.get('cols', []):
                    if col == merge_col:
                        continue
                    cols_to_select_temp.append(f'{alias}."{col}"')
                    app.logger.debug(f"SELECCIONANDO: {alias}.\"{col}\"")
                
                if cols_to_select_temp:
                    select_columns.extend(cols_to_select_temp)
                    join_clauses.append(f'''
                        LEFT JOIN {table_name} {alias}
                        ON p."{merge_col}" = {alias}."{merge_col}"
                    ''')
                    app.logger.info(f"✅ JOIN agregado para {filename}")
        
        base_query = f"""
            SELECT {', '.join(select_columns)}
            FROM plano p
            {' '.join(join_clauses)}
        """
        
        app.logger.info("Ejecutando consulta de consolidación con DuckDB...")
        app.logger.debug(f"QUERY:\n{base_query}")
        consolidado = conn.execute(base_query).df()
        app.logger.info(f"✅ Consolidación principal completada. DataFrame intermedio: {consolidado.shape}")
        
        # Aplicar renombres después del JOIN
        for filename, config in APP_CONFIG['STATIC_MASTERS'].items():
            rename_map = config.get('rename', {})
            for old_col, new_col in rename_map.items():
                if old_col in consolidado.columns:
                    consolidado.rename(columns={old_col: new_col}, inplace=True)
                    app.logger.debug(f"RENOMBRADO POST-JOIN: {old_col} → {new_col}")
        
        consolidado = process_dynamic_masters_pandas(consolidado, all_masters)
        
        return consolidado
        
    except Exception as e:
        app.logger.error(f"Error en consolidación con DuckDB: {e}")
        app.logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def generate_consolidated_data(uploaded_files_paths):
    return generate_consolidated_data_duckdb(uploaded_files_paths)

# --- RUTAS PRINCIPALES ---
@app.route('/')
def index():
    return render_template('index_unificado.html')

@app.route('/clear_cache')
def clear_cache():
    session.pop('_flashes', None)
    return jsonify({'success': True, 'message': 'Cache limpiado correctamente'})

@app.route('/limpiar_uploads')
def limpiar_uploads():
    try:
        limpiar_archivos_temporales()
        flash("Carpeta uploads limpiada correctamente", "success")
        app.logger.info("Limpieza manual de uploads completada")
    except Exception as e:
        flash(f"Error al limpiar uploads: {e}", "error")
        app.logger.error(f"Error en limpieza manual: {e}")
    return redirect(url_for('consolidar'))

@app.route('/consolidar')
def consolidar():
    session.pop('_flashes', None)
    return render_template('index.html')

@app.route('/validar')
def validar():
    session.pop('_flashes', None)
    return render_template('index2.html')

@app.route('/upload_consolidar', methods=['POST'])
def upload_consolidar():
    global df_global

    if 'files[]' not in request.files:
        return "Error: No se seleccionaron archivos", 400

    files = request.files.getlist('files[]')
    if not files or all(f.filename == '' for f in files):
        return "Error: No se seleccionaron archivos válidos", 400

    # OBTENER FORMATO DE EXPORTACIÓN
    export_format = request.form.get('export_format', 'xlsx')  # Por defecto Excel

    uploaded_files_paths = []
    temp_dirs_to_cleanup = []

    try:
        for file in files:
            if file.filename == '':
                continue

            if file.filename.lower().endswith('.zip'):
                try:
                    temp_dir = tempfile.mkdtemp(prefix='zip_extract_', dir=UPLOAD_FOLDER)
                    temp_dirs_to_cleanup.append(temp_dir)
                    with zipfile.ZipFile(file.stream, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                        app.logger.info(f"ZIP extraído en: {temp_dir}")

                    for root, _, filenames in os.walk(temp_dir):
                        for filename in filenames:
                            if filename.lower().endswith('.csv'):
                                csv_path = os.path.join(root, filename)
                                uploaded_files_paths.append(csv_path)
                except zipfile.BadZipFile:
                    return f"Error: {file.filename} no es un ZIP válido.", 400
                except Exception as e:
                    app.logger.error(f"Error procesando ZIP {file.filename}: {e}")
                    return f"Error al procesar ZIP {file.filename}: {str(e)}", 400

            elif file.filename.lower().endswith('.csv'):
                filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
                file.save(filepath)
                uploaded_files_paths.append(filepath)

            else:
                app.logger.warning(f"Archivo ignorado (no es .csv ni .zip): {file.filename}")

        if not uploaded_files_paths:
            return "Error: No se encontraron archivos .csv válidos dentro de los ZIPs o subidos.", 400

        app.logger.info(f"Total de CSVs a procesar: {len(uploaded_files_paths)}")

        # PROCESAR Y GENERAR CONSOLIDADO
        consolidated_df = generate_consolidated_data(uploaded_files_paths)

        if consolidated_df is None or consolidated_df.empty:
            return "Error: No se pudo generar el consolidado (posiblemente falta archivo de trama/plano).", 500

        # GUARDAR EN MEMORIA (para validación futura si la implementas)
        df_global = consolidated_df.copy()
        app.logger.info(f"✅ DataFrame consolidado guardado en memoria: {df_global.shape}")

        # GENERAR ARCHIVO SEGÚN FORMATO
        if export_format == 'csv':
            output_filename = "Consolidado_Final.csv"
            output_buffer = io.BytesIO()
            csv_content = consolidated_df.to_csv(index=False, encoding='utf-8-sig')
            output_buffer.write(csv_content.encode('utf-8-sig'))
            output_buffer.seek(0)
            mimetype = 'text/csv; charset=utf-8'

        else:  # Excel por defecto
            output_filename = "Consolidado_Final.xlsx"
            output_buffer = io.BytesIO()
            date_only_format = 'yyyy-mm-dd'
            datetime_format = 'yyyy-mm-dd hh:mm:ss'
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                consolidated_df.to_excel(writer, index=False, sheet_name='Consolidado')
                workbook = writer.book
                sheet = writer.sheets['Consolidado']
                for col_name in DATE_COLUMNS:
                    if col_name in consolidated_df.columns:
                        col_idx = consolidated_df.columns.get_loc(col_name) + 1
                        fmt = datetime_format if col_name in ['Fecha_Registro', 'Fecha_Modificacion'] else date_only_format
                        for row_idx in range(2, sheet.max_row + 1):
                            cell = sheet.cell(row=row_idx, column=col_idx)
                            if isinstance(cell.value, pd.Timestamp):
                                cell.number_format = fmt
            output_buffer.seek(0)
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        # LIMPIAR ARCHIVOS TEMPORALES (solo si todo salió bien)
        limpiar_archivos_especificos(uploaded_files_paths)
        for temp_dir in temp_dirs_to_cleanup:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                app.logger.info(f"Directorio temporal eliminado: {temp_dir}")

        # DEVOLVER EL ARCHIVO REAL
        return send_file(
            output_buffer,
            mimetype=mimetype,
            as_attachment=True,
            download_name=output_filename
        )

    except Exception as e:
        app.logger.error(f"Error inesperado en consolidación: {traceback.format_exc()}")
        # Limpiar en caso de error
        limpiar_archivos_especificos(uploaded_files_paths)
        for temp_dir in temp_dirs_to_cleanup:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
        return f"Error interno del servidor: {str(e)}", 500

# --- RUTAS PARA VALIDACIÓN DE ERRORES ---
@app.route('/upload_validar', methods=['POST'])
def upload_validar():
    global df_global
    
    if 'file' not in request.files:
        return jsonify({'error': 'No se seleccionó ningún archivo'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No se seleccionó ningún archivo'}), 400
    
    # ACEPTAR TANTO EXCEL COMO CSV
    if file and (file.filename.lower().endswith('.xlsx') or file.filename.lower().endswith('.csv')):
        try:
            if file.filename.lower().endswith('.xlsx'):
                # Procesar Excel (comportamiento original)
                df = pd.read_excel(file)
            else:
                # Procesar CSV
                # Guardar temporalmente el archivo para detectar separador
                temp_path = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
                file.save(temp_path)
                
                # Detectar separador y cargar CSV
                separator = detect_separator(temp_path)
                df = pd.read_csv(temp_path, sep=separator, encoding='latin1', dtype=str, keep_default_na=False, engine='python')
                
                # Limpiar archivo temporal
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            
            # PROCESAR CON LAS FUNCIONES CORREGIDAS
            df = procesar_dataframe(df)
            df = formatear_fechas(df)
            df_global = df
            data = preparar_datos_para_frontend(df)
            return jsonify({'success': True, 'message': 'Archivo cargado correctamente', 'data': data})
        except Exception as e:
            app.logger.error(f"Error en upload_validar: {str(e)}")
            app.logger.error(traceback.format_exc())
            return jsonify({'error': f'Error al procesar el archivo: {str(e)}'}), 400
    else:
        return jsonify({'error': 'Solo se permiten archivos Excel (.xlsx) o CSV (.csv)'}), 400

@app.route('/filter/<filter_type>')
def apply_filter(filter_type):
    global df_global
    if df_global is None:
        return jsonify({'error': 'No hay datos cargados'}), 400
    
    try:
        # Asegurarse de que las columnas necesarias estén presentes
        df_temp = df_global.copy()
        
        # Agregar columnas faltantes si es necesario
        if 'Error' not in df_temp.columns:
            df_temp['Error'] = ''
        
        # Aplicar el filtro correspondiente
        if filter_type in ['duplicados', 'fechas_invalidas', 'documentos_invalidos']:
            conn = get_duckdb_connection()
            conn.register('df_global', df_temp)
            
            if filter_type == 'duplicados':
                query = """
                    SELECT *, COUNT(*) OVER (PARTITION BY Id_Cita) as count_duplicates
                    FROM df_global 
                    WHERE Id_Cita IN (
                        SELECT Id_Cita 
                        FROM df_global 
                        GROUP BY Id_Cita 
                        HAVING COUNT(*) > 1
                    )
                """
            elif filter_type == 'fechas_invalidas':
                query = """
                    SELECT * FROM df_global 
                    WHERE Fecha_Atencion IS NULL 
                       OR Fecha_Atencion = ''
                       OR TRY_CAST(Fecha_Atencion AS DATE) IS NULL
                """
            elif filter_type == 'documentos_invalidos':
                query = """
                    SELECT * FROM df_global 
                    WHERE Numero_Documento_Paciente IS NULL 
                       OR Numero_Documento_Paciente = ''
                       OR LENGTH(TRIM(Numero_Documento_Paciente)) < 3
                """
            
            df_filtrado = conn.execute(query).df()
            conn.close()
            
            # Asegurar que tenga columna Error
            if 'Error' not in df_filtrado.columns:
                df_filtrado['Error'] = 'Error detectado'
                
        else:
            # Usar las funciones de validación
            df_filtrado = aplicar_filtro(df_temp, filter_type)
        
        return procesar_resultado_filtrado(df_filtrado, filter_type)
        
    except Exception as e:
        app.logger.error(f"Error al aplicar filtro {filter_type}: {str(e)}")
        app.logger.error(traceback.format_exc())
        return jsonify({'error': f'Error al aplicar filtro: {str(e)}'}), 500

def procesar_resultado_filtrado(df_filtrado, filter_type):
    if df_filtrado.empty:
        return jsonify({'success': True, 'message': 'No se encontraron errores', 
                       'data': {'columns': [], 'data': [], 'total_records': 0, 'shown_records': 0}})
    
    df_filtrado = formatear_fechas(df_filtrado)
    data = preparar_datos_para_frontend(df_filtrado)
    app.config['df_filtrado'] = df_filtrado
    return jsonify({'success': True, 
                   'message': f'Filtro {filter_type} aplicado: {len(df_filtrado)} errores encontrados', 
                   'data': data})


@app.route('/download_errores')
def download_errores():
    try:
        df_filtrado = app.config.get('df_filtrado')
        if df_filtrado is None or df_filtrado.empty:
            return jsonify({'error': 'No hay datos filtrados para descargar'}), 400
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            from funciones_procesamiento import COLUMNAS_BASE
            cols = [c for c in COLUMNAS_BASE if c in df_filtrado.columns]
            df_filtrado[cols].to_excel(writer, index=False, sheet_name="ErroresFiltrados")
            worksheet = writer.sheets["ErroresFiltrados"]
            for i, col in enumerate(cols):
                max_len = max(df_filtrado[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(max_len, 50))
        
        output.seek(0)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"ErroresFiltrados_{timestamp}.xlsx"
        
        return send_file(
            io.BytesIO(output.read()),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({'error': f'Error al generar archivo: {str(e)}'}), 500
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
