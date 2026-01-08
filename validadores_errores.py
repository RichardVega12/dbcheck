import pandas as pd
import numpy as np
from config_tipos import INTEGER_COLUMNS, DECIMAL_COLUMNS, STRING_COLUMNS

def convertir_tipos_validacion(df):
    """
    Convierte las columnas a los tipos correctos para las validaciones
    """
    df = df.copy()
    
    # Convertir columnas enteras usando la configuración centralizada
    for col in INTEGER_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # Convertir columnas decimales usando la configuración centralizada
    for col in DECIMAL_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Columnas de texto - asegurar que sean strings
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('').str.strip()
    
    return df

def errores_generales(df):
    """
    Detecta errores generales en el DataFrame consolidado.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        # Buscar la columna Id_Cita (puede tener BOM)
        id_cita_column = None
        for col in df.columns:
            if 'Id_Cita' in col:
                id_cita_column = col
                break
        
        if id_cita_column is None:
            # Aplicar mask1 directamente sin agrupar por Id_Cita
            mask1 = (df["Id_Ups"] == "302101") & (~df["Ficha_Familiar"].str.startswith("APP", na=False)) & ((df["Id_Condicion_Establecimiento"] != "C") | (df["Id_Condicion_Servicio"] != "C"))
            df.loc[mask1, "Error"] = "Condición de establecimiento y servicio deben ser 'C' (Continuadores)"
        else:
            # Condición: si el establecimiento o servicio no son "C"
            mask1 = (df["Id_Ups"] == "302101") & (~df["Ficha_Familiar"].str.startswith("APP", na=False)) & ((df["Id_Condicion_Establecimiento"] != "C") | (df["Id_Condicion_Servicio"] != "C"))
            
            if mask1.sum() > 0:
                # Obtener las citas que tienen al menos un registro que cumple mask1
                citas_con_error = df[mask1][id_cita_column].unique()
                
                # Crear máscara para el PRIMER registro de cada cita problemática
                mask_citas_problematicas = df[id_cita_column].isin(citas_con_error)
                mask_primer_registro = mask_citas_problematicas & ~df.duplicated(subset=[id_cita_column], keep="first")
                
                # Asignar el error SOLO al primer registro de cada cita problemática
                df.loc[mask_primer_registro, "Error"] = "Condición de establecimiento y servicio deben ser 'C' (Continuadores)"

        mask2 = (df["Codigo_Item"] == "Z019") & (df["Valor_Lab"] == "DNT") & (df["Mes"] > 7)
        df.loc[mask2, "Error"] = "EL VALOR LAB TIENE QUE SER DIFERENTE DNT"
     
        mask3 = (df["Codigo_Item"].isin(["85018", "85018.01"]) & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == "")))
        df.loc[mask3, "Error"] = "Verificar el numero de Dosaje"

        mask4 = (df["Codigo_Item"].isin(["C0011", "C0011.01"])) & (df["Tipo_Diagnostico"] == "R")
        df.loc[mask4, "Error"] = "Visita Domiciliaria no puede Tipo_Dx R"

        mask5 = (df["Codigo_Item"] == "99199.22") & (df["Mes"] > 8) & (df["Valor_Lab"].isin(["N", "A"]) | df["Valor_Lab"].isna())
        df.loc[mask5, "Error"] = "Deben de tener valores de sistólica y diastólica"

        mask6 = (df["Codigo_Item"].isin(["99381.01","99381","99382","99383","88141","85018","59430","99403", "99199.17","99402.08","99199.22","D1310","D1330"])) & (df["Tipo_Diagnostico"] != "D")
        df.loc[mask6, "Error"] = "El tipo de Diagnostico no puede ser R"

        mask7 = (df["Codigo_Item"] == "84152") & (df["Genero"] == "F")
        df.loc[mask7, "Error"] = "Diagnostico solo para varones"

        mask8 = (df["Codigo_Item"] == "O260") & (df["Genero"] == "M")
        df.loc[mask8, "Error"] = "Diagnostico solo para mujeres"

        mask9 = (df["Codigo_Item"] == "Z010") & ((~df["Valor_Lab"].isin(["N", "A"])) | (df["Valor_Lab"] == ""))
        df.loc[mask9, "Error"] = "El Valor Lab debe ser N o A"

        mask10 = (df["Codigo_Item"] == "84153") & (df["Genero"] == "F")
        df.loc[mask10, "Error"] = "Cambiar por el codigo 84152"

    except Exception as e:
        print(f"Error en validación general: {e}")

    return df[df["Error"] != ""].copy()

def errores_adolescente(df):
    """
    Detecta errores específicos para el servicio de Adolescente.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        # 1. Excluir citas que ya tienen D509 o O990 (estas NO se revisan para esta regla)
        citas_con_diagnostico = df[df["Codigo_Item"].isin(["D509", "O990","Z3591", "Z3592", "Z3593", "Z3594"])]["Id_Cita"].unique()

        # Convertir Anio_Actual_Paciente a numérico para comparación
        anio_actual_numeric = pd.to_numeric(df["Anio_Actual_Paciente"], errors='coerce')
        
        # 2. Definir mask1: SOLO en citas SIN diagnóstico de anemia/embarazo
        mask1 = (
            (~df["Id_Cita"].isin(citas_con_diagnostico)) &  # <-- Exclusión clave
            (df["Codigo_Item"] == "99199.26") &
            (df["Valor_Lab"] != "TA") &
            (anio_actual_numeric.between(12, 17))
        )

        # 3. Marcar error solo donde mask1 es True
        df.loc[mask1, "Error"] = "VERIFICAR SUPLEMENTACION EN ADOLESCENTES QUE NO SEAN TA"

    except Exception as e:
        print(f"Error en validación adolescente: {e}")

    return df[df["Error"] != ""].copy()

def errores_obstetricia(df):
    """
    Detecta errores específicos para el servicio de Obstetricia.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = (df["Codigo_Item"] == "99208.13") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "4")
        df.loc[mask1, "Error"] = "El codigo 99208.13 con DX R solo acepta el campo LAB con valor 4"

        mask2 = (df["Codigo_Item"] == "99208.13") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "1")
        df.loc[mask2, "Error"] = "El codigo 99208.13 con DX D solo acepta el campo LAB con valor 1 o cambiar el Diagnostico a R SI EL LAB ES 4"

        mask3 = (df["Codigo_Item"] == "99208.02") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "10")
        df.loc[mask3, "Error"] = "El codigo 99208.02 con DX D solo acepta el campo LAB con valor 10 si el valor lab es 30, corregir DX R"

        mask4 = (df["Codigo_Item"] == "99208.02") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "30")
        df.loc[mask4, "Error"] = "El codigo 99208.02 con DX R solo acepta el campo LAB con valor 30 si el valor es 10 poner D"

        mask5 = (df["Codigo_Item"] == "99208.06") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "30")
        df.loc[mask5, "Error"] = "El codigo 99208.06 con DX R solo acepta el campo LAB con valor 30"

        mask6 = (df["Codigo_Item"] == "99208.04") & (df["Tipo_Diagnostico"].isin(["D", "R"])) & (df["Valor_Lab"] != "1")
        df.loc[mask6, "Error"] = "El codigo 99208.04 solo acepta el campo LAB con valor 1"

        mask7 = (df["Codigo_Item"] == "99208.05") & (df["Tipo_Diagnostico"].isin(["D", "R"])) & (df["Valor_Lab"] != "1")
        df.loc[mask7, "Error"] = "El codigo 99208.05 solo acepta el campo LAB con valor 1"

        mask8 = (df["Codigo_Item"] == "99208.06") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "10")
        df.loc[mask8, "Error"] = "El codigo 99208.06 con DX D solo acepta el campo LAB con valor 10"

        mask9 = (df["Codigo_Item"] == "92100") & (~df["Valor_Lab"].isin(["N", "A"]))
        df.loc[mask9, "Error"] = "EL Valor_Lab tiene que ser N o A"

        mask10 = (
            df["Codigo_Item"].isin(["86703", "87342", "86780", "87340", "86703.01", "86703.02", "86318.01", "86803.01"]) &
            (df["Tipo_Diagnostico"] == "D") &
            (~df["Valor_Lab"].isin(["RP", "RN"]))
        )
        df.loc[mask10, "Error"] = "El campo LAB debe ser RN= Resultado Negativo o RP= Resultado Positivo"

        mask11 = (df["Codigo_Item"] == "59401.06") & (~df["Valor_Lab"].isin(["1", "2", "3","TA"]))
        df.loc[mask11, "Error"] = "Plan de Parto debe tener valor_lab 1,2,3 o TA"

        mask12 = (((df["Codigo_Item"] == "80055.01") & ((df["Valor_Lab"] != "1") | (df["Valor_Lab"].isna()))) |
                ((df["Codigo_Item"] == "80055.02") & ((df["Valor_Lab"] != "2") | (df["Valor_Lab"].isna()))))
        df.loc[mask12, "Error"] = "Corregir la primera bateria 80055.01 Con lab 1 y segunda bateria 80055.02 con lab 2"

        mask13 = df["Codigo_Item"].isin(["86703.01", "86703.02", "86780", "86318.01", "87342"]) & (~df["Valor_Lab"].isin(["RN", "RP"]))
        df.loc[mask13, "Error"] = "Valor_Lab solo debe de Tener RN y RP"

        mask14 = (df["Codigo_Item"].isin(["88141","88141.01", "99386.03"]) & (~df["Valor_Lab"].isin(["N", "A"]) & df["Valor_Lab"].notna() & (df["Valor_Lab"] != "")))
        df.loc[mask14, "Error"] = "El Valor debe de ser Normal, Anormal o vacio"

        mask15 = (df["Codigo_Item"] == "99208.14") & (~df["Valor_Lab"].isin(["RSA", "RSM", "RSR"]))
        df.loc[mask15, "Error"] = "EL Valor_Lab tiene que ser RSA,RSR o RSM"

        mask16 = df["Codigo_Item"].isin(["59430"]) & (~df["Valor_Lab"].isin(["1", "2"]))
        df.loc[mask16, "Error"] = "EL Valor_Lab tiene que tener valores 1 o 2"

        mask17 = (df["Codigo_Item"] == "59401.05") & (~df["Valor_Lab"].isin(["1", "2","3","4"]))
        df.loc[mask17, "Error"] = "EL Valor_Lab tiene que ser 1,2,3 o 4"

        mask18 = (df["Codigo_Item"] == "99401.33") & ~(df["Valor_Lab"].isin(["1", "2"]) | (df["Valor_Lab"] == "") | df["Valor_Lab"].isna())
        df.loc[mask18, "Error"] = "EL Valor_Lab tiene que tener 1,2 o vacio"

        mask19 = (df["Codigo_Item"] == "99401.34") & ~(df["Valor_Lab"].isin(["1", "2","rma","rsa"]) | (df["Valor_Lab"] == "") | df["Valor_Lab"].isna())
        df.loc[mask19, "Error"] = "EL Valor_Lab tiene que tener 1,2,rma,rsa o vacio"

        mask20 = df["Codigo_Item"].isin(["87621"]) & (~df["Valor_Lab"].isin(["1", "2","N","A"]))
        df.loc[mask20, "Error"] = "EL Valor_Lab tiene que tener valores 1, 2, 'N', 'A'"

        mask21 = (df["Genero"] == "M") & (df["Valor_Lab"].isin(["Z349", "Z3593", "Z359", "Z3491", "Z3592", "88141", "84152", "Z320", "N952", "O990", "Z374", "N951", "Z391", "C530", "M800", "O987", "Z014", "O261", "Z392", "Z641", "O479", "Z370", "N939", "N771", "O240", "B373", "O260", "N872", "N72X", "Z373"]))
        df.loc[mask21, "Error"] = "El genero debe de ser Femenino"

        mask22 = (df["Genero"] == "F") & (df["Codigo_Item"].isin(["N40X", "N433", "C61X", "N481"]))
        df.loc[mask22, "Error"] = "El genero debe de ser Masculino"

        mask23 = (df["Codigo_Item"].isin(["99386.03"]) & (~df["Valor_Lab"].isin(["N", "A"])))
        df.loc[mask23, "Error"] = "El Valor debe de ser Normal o Anormal"

    except Exception as e:
        print(f"Error en validación obstetricia: {e}")

    return df[df["Error"] != ""].copy()

def errores_dental(df):
    """
    Detecta errores específicos para el servicio Dental.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = df["Codigo_Item"].isin([
            "D5110", "D5213", "D5120", "D5214", "D5130", "D5225", "D5140",
            "D5226", "D5211", "D5860", "D5212", "D5861"]) & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == ""))
        df.loc[mask1, "Error"] = "El Valor_Lab no puede estar vacio"

        mask2 = df["Codigo_Item"].isin(["D1310", "D1330"]) & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == ""))
        df.loc[mask2, "Error"] = "El Valor_Lab no puede estar vacio"

        mask3 = df["Codigo_Item"].isin(["D1206"]) & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == ""))
        df.loc[mask3, "Error"] = "El Valor_Lab no puede estar vacio tiene que ser 1 o 2"

        mask4 = df["Codigo_Item"].isin(["D1351"]) & (~df["Valor_Lab"].isin(["1", "2", "3","4","FIN"]))
        df.loc[mask4, "Error"] = "El Valor_Lab solo debe llevar lab 1, 2, 3, 4 o FIN"

    except Exception as e:
        print(f"Error en validación dental: {e}")

    return df[df["Error"] != ""].copy()

def errores_inmunizaciones(df):
    """
    Detecta errores específicos para el servicio de Inmunizaciones.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = df["Codigo_Item"] == "90676"
        df.loc[mask1, "Error"] = "Vacuna Antirrabica es 90675"

        # Buscar y normalizar Id_Cita
        id_cita_column = None
        for col in df.columns:
            if 'Id_Cita' in col:
                id_cita_column = col
                if col != 'Id_Cita':
                    df.rename(columns={col: 'Id_Cita'}, inplace=True)
                break
        
        if id_cita_column is None:
            df['Id_Cita'] = 'DUMMY_' + df.index.astype(str)

        # VALIDACIÓN PARA CÓDIGO 90675
        citas_con_90675 = df[df["Codigo_Item"] == "90675"]["Id_Cita"].unique()
        
        for cita in citas_con_90675:
            registros = df[(df["Id_Cita"] == cita) & (df["Codigo_Item"] == "90675")]
            valores = registros["Valor_Lab"].astype(str).str.strip().unique()
            
            numericos = ['1', '2', '3', '4', '5']
            pre_post = ['PRE', 'POS']
            
            tiene_num = any(v in numericos for v in valores)
            tiene_pp = any(v in pre_post for v in valores)
            count = len(registros)
            
            # Aplicar reglas
            if count == 1:
                if tiene_num:
                    df.loc[registros.index, "Error"] = "FALTA AGREGAR PRE O POS"
                elif tiene_pp:
                    df.loc[registros.index, "Error"] = "FALTA AGREGAR VALOR NUMÉRICO (1,2,3,4,5)"
                else:
                    df.loc[registros.index, "Error"] = "Valor_Lab inválido"
                    
            elif count == 2:
                if not (tiene_num and tiene_pp):
                    df.loc[registros.index, "Error"] = "Debe tener un valor numérico y un PRE/POST"
                    
            elif count > 2:
                df.loc[registros.index, "Error"] = "Demasiados registros 90675 - Solo 2 permitidos"
            
            # Validar valores individuales
            for idx in registros.index:
                val = str(df.loc[idx, "Valor_Lab"]).strip()
                if val not in numericos + pre_post:
                    df.loc[idx, "Error"] = f"Valor '{val}' inválido - Use 1,2,3,4,5 o PRE,POS"

    except Exception as e:
        print(f"Error en validación inmunizaciones: {e}")

    return df[df["Error"] != ""].copy()

def errores_cred(df):
    """
    Detecta errores específicos para el servicio de Area de Cred.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = ((df["Codigo_Item"].isin(["99199.17", "99199.19"])) & 
                (~df["Valor_Lab"].isin(["1", "2", "3", "4", "5", "6", "7","TA"])) &
                (df["Valor_Lab"] != ""))
        df.loc[mask1, "Error"] = "Verificar el numero de suplementacion"

        mask2 = ((df["Codigo_Item"].isin(["85018", "85018.01"])) & (df["Lote"] == "CED") & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == "")))
        df.loc[mask2, "Error"] = "Lab Vacio, tiene que ir numero de tamizaje"

        mask3 = ((df["Codigo_Item"].isin(["85018", "85018.01"])) & (df["Hemoglobina"].isna()) & (df["Lote"] == "CED") & (df["Valor_Lab"] != ""))
        df.loc[mask3, "Error"] = "Lab Vacio, No tiene Valor de Hemoglobina"

        mask4 = ((df["Codigo_Item"].isin(["99801"])) & (df["Lote"] == "CED") & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == "")))
        df.loc[mask4, "Error"] = "Plan de Atencion integral Vacio"

        mask5 = ((df["Codigo_Item"].isin(["99381.01","99381","99382","99383"])) & (df["Lote"] == "CED") & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == "")))
        df.loc[mask5, "Error"] = "Nro de Control Vacio"

        mask6 = ((df["Codigo_Item"] == "R620") & (df["Lote"] == "CED") & (~df["Valor_Lab"].isin(["MOT","LEN"])))
        df.loc[mask6, "Error"] = "VALORES DEBEN SER MOT O LEN"

        mask7 = ((df["Codigo_Item"].isin(["99199.28"])) & (~df["Valor_Lab"].isin(["1","2"])))
        df.loc[mask7, "Error"] = "Desparasitación solo debe ser 1 o 2"

        #
       # Sólo evaluar filas con Codigo_Item == "Z001"
        mask8 = ((df["Codigo_Item"] == "Z001") & ((df["Tipo_Diagnostico"] != "D") | (df["Valor_Lab"].fillna("") != "")))
        df.loc[mask8, "Error"] = ("Error: Para Z001 solo es válido Tipo_Diagnostico='D' y Valor_Lab vacío")

        mask9 = ((df["Codigo_Item"] == "99199.27") & (~df["Valor_Lab"].isin(["VA1", "VA2"])))
        df.loc[mask9, "Error"] = "Suplementacion con vitamina A es VA1 o VA2"

    except Exception as e:
        print(f"Error en validación cred: {e}")

    return df[df["Error"] != ""].copy()

def errores_nutricion(df):
    """
    Detecta errores específicos para el servicio de Nutrición.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = ((df["Codigo_Item"] == "R628") & (~df["Valor_Lab"].isin(["TP","PR"])))
        df.loc[mask1, "Error"] = "EL R628 maneja campo lab TP, verificar si el codigo es z724 P/E y T/E"

        mask2 = ((df["Codigo_Item"].isin(["D509","O990"])) & (df["Tipo_Diagnostico"].isin(["D", "R"])) & (~df["Valor_Lab"].isin(["LEV", "MOD", "SEV","PR"])))
        df.loc[mask2, "Error"] = "El Valor de Anemia es 'LEV', 'MOD', 'SEV'"

    except Exception as e:
        print(f"Error en validación nutricion: {e}")

    return df[df["Error"] != ""].copy()

def errores_psicologia(df):
    """
    Detecta errores específicos para el servicio de Psicología.
    """
    df = convertir_tipos_validacion(df)
    df["Error"] = ""

    try:
        mask1 = ((df["Codigo_Item"].isin(["F700", "F710", "F791", "F711", "F721", "F709", "F719", "F701", "F799", "F729", "F720", "F708", "F789", "F790", "F798", "F718", "F739", "F781", "F788"])) & (df["Tipo_Diagnostico"].isin(["D"])))
        df.loc[mask1, "Error"] = "Cambiar el Tipo de Dx Retraso mental a R"

       
    except Exception as e:
        print(f"Error en validación psicologia: {e}")

    return df[df["Error"] != ""].copy()

def errores_secuencia_dx(df):
    """
    Valida secuencia de diagnósticos crónicos y condiciones especiales:
    → Solo se permite UN ÚNICO 'D' por paciente en todo el historial.
    Reglas:
      • Duplicados 'D' en el mismo mes → todos son error (EXCEPTO mismos Id_Cita)
      • 'D' posterior al primer 'D' cronológico → error (EXCEPTO mismos Id_Cita)
    """
    df = convertir_tipos_validacion(df).copy()
    
    # SOLUCIÓN ROBUSTA: Buscar y normalizar la columna Id_Cita
    id_cita_column = None
    for col in df.columns:
        if 'Id_Cita' in col:
            id_cita_column = col
            # Renombrar si tiene BOM o formato diferente
            if col != 'Id_Cita':
                df.rename(columns={col: 'Id_Cita'}, inplace=True)
                id_cita_column = 'Id_Cita'
            break
    
    # Si no se encontró Id_Cita, crear una columna dummy con valores únicos
    if id_cita_column is None:
        print("⚠️ ADVERTENCIA: No se encontró columna Id_Cita, creando columna dummy")
        df['Id_Cita'] = 'DUMMY_' + df.index.astype(str)
        id_cita_column = 'Id_Cita'
    
    # Asegurar que Id_Cita sea string
    df['Id_Cita'] = df['Id_Cita'].astype(str).fillna('').str.strip()
    
    if "Error" not in df.columns:
        df["Error"] = ""

    # ================== CONFIGURACIÓN CENTRALIZADA ==================
    DIAG_SECUENCIA = {
        "ANEMIA": {
            "type": "exact",
            "codes": ["D509"],
            "nombre": "ANEMIA"
        },
        "HIPERTENSION": {
            "type": "exact",
            "codes": ["I10X"],
            "nombre": "HIPERTENSIÓN ARTERIAL"
        },
        "DIABETES": {
            "type": "exact",
            "codes": ["E111","E112","E113","E114","E115","E116","E117","E118","E119",
                      "E141","E142","E143","E144","E145","E146","E147","E148","E149"],
            "nombre": "DIABETES MELLITUS"
        },
        "VIOLENCIA": {
            "type": "exact",
            "codes": ["T740","T741","T742","T743","T748","T749",
                      "Y040","Y050","Y058","Y060","Y061","Y062","Y068",
                      "Y070","Y071","Y072","Y078","Y079"],
            "nombre": "VIOLENCIA (FÍSICA, SEXUAL, PSICOLÓGICA, MALTRATO)"
        },
        "DEPRESION": {
            "type": "exact",
            "codes": ["F314","F317","F319","F320","F321","F322","F323","F328","F329",
                      "F330","F331","F332","F334","F339","F341","F413"],
            "nombre": "DEPRESIÓN"
        },
        "AUTISMO": {
            "type": "exact",
            "codes": ["F840","F841","F845","F848","F849"],
            "nombre": "TRASTORNO DEL ESPECTRO AUTISTA"
        },
        "SINDROME_DOWN": {
            "type": "exact",
            "codes": ["Q900","Q909"],
            "nombre": "SÍNDROME DE DOWN"
        },
        "CONDUCTA_SUICIDA": {
            "type": "exact",
            "codes": ["X780","X788","X849"],
            "nombre": "INTENTO/CONDUCTA SUICIDA"
        },
        "ANSIEDAD": {
            "type": "exact",
            "codes": ["F400","F401","F402","F408","F409","F410","F411","F412","F413","F418","F419",
                      "F420","F421","F422","F428","F429","F430","F431","F432","F438","F439",
                      "F440","F445","F447","F448","F449","F450","F451","F452","F458","F459","F489"],
            "nombre": "TRASTORNOS DE ANSIEDAD"
        },
        "HEPATITIS": {
            "type": "exact",
            "codes": ["B160","B169","B180","B181"],
            "nombre": "HEPATITIS B"
        }
    }
    # ==================================================================

    # Asegurar formato correcto de fecha (tus datos vienen en dd/mm/yyyy)
    df["Fecha_Atencion"] = pd.to_datetime(df["Fecha_Atencion"], format='%d/%m/%Y', errors='coerce')

    for key, config in DIAG_SECUENCIA.items():
        # Construir máscara
        mask = df["Codigo_Item"].isin(config["codes"]) & (df["Tipo_Diagnostico"] != "P")
        if not mask.any():
            continue

        sub = df[mask].copy()
        sub["Anio_Mes"] = sub["Fecha_Atencion"].dt.to_period('M')
        sub = sub.sort_values(["Numero_Documento_Paciente", "Fecha_Atencion"])

        es_D = sub["Tipo_Diagnostico"] == "D"
        if not es_D.any():
            continue

        # Primer 'D' cronológico por paciente
        primer_D_idx = sub[es_D].groupby("Numero_Documento_Paciente")["Fecha_Atencion"].idxmin()

        # Detectar errores
        errores = pd.Series(False, index=sub.index)

        # 1. Duplicados en el mismo mes (EXCLUYENDO mismos Id_Cita)
        dup_mes = sub[es_D].duplicated(
            subset=["Numero_Documento_Paciente", "Anio_Mes", "Codigo_Item"], 
            keep=False
        )
        
        # Excluir los que tienen el mismo Id_Cita (válidos)
        mismos_id_cita = sub[es_D].duplicated(
            subset=["Numero_Documento_Paciente", "Anio_Mes", "Codigo_Item", "Id_Cita"], 
            keep=False
        )
        
        # Solo son error los duplicados que NO tienen el mismo Id_Cita
        dup_mes_error = dup_mes & ~mismos_id_cita
        errores |= (es_D & dup_mes_error)

        # 2. 'D' posteriores al primero (EXCLUYENDO mismos Id_Cita del primer diagnóstico)
        posteriores = es_D & (~sub.index.isin(primer_D_idx))
        
        # Identificar cuáles de los posteriores tienen el mismo Id_Cita que el primer diagnóstico
        # Obtener los Id_Cita del primer diagnóstico de cada paciente
        primer_D_data = sub.loc[primer_D_idx, ["Numero_Documento_Paciente", "Id_Cita"]]
        primer_D_dict = primer_D_data.set_index("Numero_Documento_Paciente")["Id_Cita"].to_dict()
        
        # Para cada registro posterior, verificar si comparte Id_Cita con su primer diagnóstico
        posteriores_con_mismo_id_cita = pd.Series(False, index=sub.index)
        
        for idx in sub[posteriores].index:
            paciente = sub.loc[idx, "Numero_Documento_Paciente"]
            id_cita_actual = sub.loc[idx, "Id_Cita"]
            primer_id_cita = primer_D_dict.get(paciente)
            
            if primer_id_cita is not None and id_cita_actual == primer_id_cita:
                posteriores_con_mismo_id_cita[idx] = True
        
        # Excluir del error los que comparten Id_Cita con el primer diagnóstico
        posteriores_error = posteriores & ~posteriores_con_mismo_id_cita
        errores |= posteriores_error

        if errores.any():
            mensaje = (f"{config['nombre']} con Tipo D inválido: "
                       f"solo se permite un único 'D' por paciente en todo su historial. "
                       f"Duplicados en el mismo mes o 'D' posteriores al primero son error.")
            df.loc[sub[errores].index, "Error"] = mensaje

    return df[df["Error"] != ""].copy()

# Diccionario de funciones de validación
FILTER_FUNCTIONS = {
    'generales': errores_generales,
    'dental': errores_dental,
    'adolescente': errores_adolescente,
    'obstetricia': errores_obstetricia,
    'inmunizaciones': errores_inmunizaciones,
    'cred': errores_cred,
    'nutricion': errores_nutricion,
    'psicologia': errores_psicologia,
    'Error_secuencia_Dx': errores_secuencia_dx

}

def aplicar_filtro(df, filter_type):
    """
    Aplica un filtro de errores específico al DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a validar
        filter_type (str): Tipo de filtro a aplicar
    
    Returns:
        pd.DataFrame: DataFrame con los errores encontrados
    """
    if filter_type not in FILTER_FUNCTIONS:
        raise ValueError(f"Tipo de filtro no válido: {filter_type}")
    
    return FILTER_FUNCTIONS[filter_type](df)

def obtener_funciones_validacion():
    """
    Retorna el diccionario de funciones de validación disponibles.
    """
    return FILTER_FUNCTIONS

def ejecutar_todos_los_filtros(df):
    """
    Ejecuta todos los filtros de validación y retorna un DataFrame consolidado con todos los errores.
    
    Args:
        df (pd.DataFrame): DataFrame a validar
    
    Returns:
        pd.DataFrame: DataFrame con todos los errores encontrados por todos los filtros
    """
    todos_errores = []
    
    for nombre_filtro, funcion in FILTER_FUNCTIONS.items():
        try:
            errores = funcion(df)
            if not errores.empty:
                errores['Tipo_Filtro'] = nombre_filtro
                todos_errores.append(errores)
        except Exception as e:
            print(f"Error ejecutando filtro {nombre_filtro}: {e}")
    
    if todos_errores:
        return pd.concat(todos_errores, ignore_index=True)
    else:
        return pd.DataFrame()