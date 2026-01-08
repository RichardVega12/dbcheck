--niñas de 12 a 59 meses
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99401.25' AND B.Valor_Lab='1' AND B.Tipo_Diagnostico='D'  THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab='2' AND B.Tipo_Diagnostico='D' THEN 2
END) = 2


--condicion gestante
--obligatorio ultima dia de regla
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99402.03' AND B.Valor_Lab IN('1','2') AND B.Tipo_Diagnostico='D'  THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab IN('1','2') AND B.Tipo_Diagnostico='D' THEN 2

END) = 2

--CONDICION PUERPERA
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99401.08' AND B.Valor_Lab IN('1','2') AND B.Tipo_Diagnostico='D' THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab IN('1','2') AND B.Tipo_Diagnostico='D' THEN 2

END) = 2


--PROMOCION DE LA SALUD - TBC
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99401.36' AND B.Valor_Lab IN('1','2') AND B.Tipo_Diagnostico='D' THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab IN('161') AND B.Tipo_Diagnostico='D' THEN 2

END) = 2


--PROMOCION DE LA SALUD - VIH
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99401.15' AND B.Valor_Lab='1' AND B.Tipo_Diagnostico='D' THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab='162' AND B.Tipo_Diagnostico='D' THEN 2

END) = 2


--2da consejeria VIH
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = '99401.19' AND B.Valor_Lab='2' AND B.Tipo_Diagnostico='D' THEN 1
        WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab='162' AND B.Tipo_Diagnostico='D' THEN 2

END) = 2


--METAXENICAS Y ZOONOSIS
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item = 'W540' AND B.Valor_Lab='LEV' AND B.Tipo_Diagnostico='R' AND B.Id_Correlativo = 1 THEN 1
		WHEN B.Codigo_Item = 'W540' AND B.Valor_Lab='C' AND B.Tipo_Diagnostico='R' AND B.Id_Correlativo = 2 THEN 2
		WHEN B.Codigo_Item = '99401' AND B.Valor_Lab='1' AND B.Tipo_Diagnostico='D'  THEN 3
        WHEN B.Codigo_Item = '99199.11' AND B.Valor_Lab IS NULL AND B.Tipo_Diagnostico='D' THEN 4
		WHEN B.Codigo_Item = 'C0011' AND B.Valor_Lab IS NULL AND B.Tipo_Diagnostico='D' THEN 5

END) = 5

--INMUNIZACIONES
--MENORES DE 12 meses

HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item IN ("90713","90723","90670","90681") AND B.Valor_Lab IS NOT NULL AND B.Tipo_Diagnostico='D'  THEN 1
END) = 1

--EDAD=1
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item IN ("90707","90670","90657","90716") AND B.Valor_Lab IS NOT NULL AND B.Tipo_Diagnostico='D'  THEN 1
END) = 1

--EDAD=4
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item IN ("90701","90713") AND B.Valor_Lab IS NOT NULL AND B.Tipo_Diagnostico='D'  THEN 1
END) = 1

--EDAD>=59
HAVING COUNT(DISTINCT CASE
        WHEN B.Codigo_Item IN ("90658","90670") AND B.Valor_Lab IS NOT NULL AND B.Tipo_Diagnostico='D'  THEN 1
END) = 1

