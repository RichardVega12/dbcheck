# config_tipos.py
# Configuración centralizada de tipos de datos para toda la aplicación

# Campos enteros
INTEGER_COLUMNS = [
    'Num_Pag', 'Num_Reg', 'Tipo_Doc_Paciente', 'Tipo_Doc_Personal', 
    'Tipo_Doc_Registrador', 'Id_Condicion', 'Anio_Actual_Paciente', 
    'Mes_Actual_Paciente', 'Dia_Actual_Paciente', 'Id_Correlativo', 
    'Id_Correlativo_Lab', 'Edad_Dias_Paciente_FechaAtencion', 
    'Edad_Meses_Paciente_FechaAtencion', 'Edad_Anios_Paciente_FechaAtencion',
    'Edad_Dias_Paciente_FechaActual', 'Edad_Meses_Paciente_FechaActual', 
    'Edad_Anios_Paciente_FechaActual', 'Mes', 'Edad_Reg'
    # ❌ REMOVER 'Id_Ups' de INTEGER_COLUMNS
]

# Campos decimales
DECIMAL_COLUMNS = [
    'Peso', 'Talla', 'Hemoglobina', 'Perimetro_Abdominal', 'Perimetro_Cefalico'
]

# Campos de texto
STRING_COLUMNS = [
    'Id_Cita', 'Id_Ups', 'Id_Financiador', 'Id_Condicion_Establecimiento', 
    'Id_Condicion_Servicio', 'Id_Turno', 'Codigo_Item', 'Id_Correlativo',
    'Id_Correlativo_Lab', 'Id_Centro_Poblado', 'Id_dosis', 'renipress', 'Valor_Lab',
    'Id_Institucion_Edu', 'Id_AplicacionOrigen', 'Alerta', 'Codigo_Red', 'Codigo_MicroRed',
    'Codigo_Unico', 'Id_Tipo_Documento_Paciente', 'Numero_Documento_Paciente', 'Id_Etnia',
    'Historia_Clinica', 'Ficha_Familiar', 'Ubigeo_Nacimiento', 'Ubigeo_Reniec', 'Ubigeo_Declarado',
    'Id_Condicion', 'Id_Profesion', 'Id_Colegio', 'Numero_Colegiatura',
    'Id_Tipo_Documento_Personal', 'Numero_Documento_Personal',
    'Id_Tipo_Documento_Registrador', 'Numero_Documento_Registrador',
    'Id_Busqueda', 'Lote', 'Tipo_Diagnostico', 'Genero'
    # ✅ 'Id_Ups' se mantiene SOLO aquí
]

# Campos de fecha
DATE_COLUMNS = [
    'Fecha_Atencion', 'Fecha_Nacimiento_Paciente', 'Fecha_Nacimiento_Personal',
    'Fecha_Nacimiento_Registrador', 'Fecha_Ultima_Regla', 'Fecha_Solicitud_Hb',
    'Fecha_Resultado_Hb', 'Fecha_Registro', 'Fecha_Modificacion'
]

# Columnas finales del consolidado (igual que antes)
FINAL_COLUMNS = [
    'Id_Cita', 'Anio', 'Mes', 'Dia', 'Fecha_Atencion', 'Lote', 'Num_Pag', 'Num_Reg',
    'Id_Ups', 'Descripcion_Ups', 'Descripcion_Sector', 'Descripcion_Disa',
    'Descripcion_Red', 'Descripcion_MicroRed', 'Codigo_Unico', 'Nombre_Establecimiento',
    'Abrev_Tipo_Doc_Paciente', 'Numero_Documento_Paciente', 'Apellido_Paterno_Paciente',
    'Apellido_Materno_Paciente', 'Nombres_Paciente', 'Fecha_Nacimiento_Paciente',
    'Genero', 'Id_Etnia', 'Descripcion_Etnia', 'Historia_Clinica', 'Ficha_Familiar',
    'Id_Financiador', 'Descripcion_Financiador', 'Descripcion_Pais',
    'Abrev_Tipo_Doc_Personal', 'Numero_Documento_Personal', 'Apellido_Paterno_Personal',
    'Apellido_Materno_Personal', 'Nombres_Personal', 'Fecha_Nacimiento_Personal',
    'Id_Condicion', 'Descripcion_Condicion', 'Id_Profesion', 'Descripcion_Profesion',
    'Id_Colegio', 'Descripcion_Colegio', 'Numero_Colegiatura',
    'Abrev_Tipo_Doc_Registrador', 'Numero_Documento_Registrador',
    'Apellido_Paterno_Registrador', 'Apellido_Materno_Registrador',
    'Nombres_Registrador', 'Fecha_Nacimiento_Registrador',
    'Id_Condicion_Establecimiento', 'Id_Condicion_Servicio', 'Edad_Reg', 'Tipo_Edad',
    'Anio_Actual_Paciente', 'Mes_Actual_Paciente', 'Dia_Actual_Paciente',
    
    # NUEVAS COLUMNAS CALCULADAS
    'Edad_Dias_Paciente_FechaAtencion', 'Edad_Meses_Paciente_FechaAtencion', 
    'Edad_Anios_Paciente_FechaAtencion', 'Edad_Dias_Paciente_FechaActual',
    'Edad_Meses_Paciente_FechaActual', 'Edad_Anios_Paciente_FechaActual',
    'Grupo_Edad',
    
    # COLUMNAS ORIGINALES CONTINUAN
    'Id_Turno', 'Codigo_Item', 'Descripcion_Item', 'Tipo_Diagnostico', 'Valor_Lab', 
    'Id_Correlativo','Id_Correlativo_Lab', 'Peso', 'Talla', 'Hemoglobina', 
    'Perimetro_Abdominal', 'Perimetro_Cefalico', 'Descripcion_Otra_Condicion', 
    'Fecha_Ultima_Regla', 'Fecha_Solicitud_Hb', 'Fecha_Resultado_Hb', 'Fecha_Registro', 
    'Fecha_Modificacion'
]