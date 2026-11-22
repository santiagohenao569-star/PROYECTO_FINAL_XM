# Diccionario de datos

Documento vivo. Las unidades que no han sido confirmadas contra la documentacion oficial de XM se dejan explicitamente como pendientes y no se inventan.

## 1. Estructura normalizada comun - CURATED

| Campo | Tipo esperado | Descripcion |
|---|---|---|
| `metrica` | string | Nombre de la metrica XM de origen. |
| `fecha` | date | Fecha de la observacion. |
| `hora` | integer/null | Hora 1-24 para metricas horarias; null para metricas diarias. |
| `entidad` | string | Identificador de la entidad reportada por XM (recurso, embalse, rio o sistema segun la metrica). |
| `valor` | double | Valor numerico convertido desde el campo de la API. La unidad depende de la metrica y debe validarse en la fuente oficial. |
| `archivo_origen` | string | Archivo RAW del cual se obtuvo el registro. |
| `year` | integer | Ano derivado de `fecha`, usado para particionamiento de CURATED. |

## 2. Metricas historicas

| Metrica | Entidad | Granularidad | Uso en proyecto | Unidad |
|---|---|---|---|---|
| `PorcVoluUtilDiar` | Embalse | Diaria | Condicion relativa de almacenamiento | Pendiente de confirmar en documentacion XM |
| `VoluUtilDiarMasa` | Embalse | Diaria | Volumen util diario | Pendiente de confirmar |
| `CapaUtilDiarMasa` | Embalse | Diaria | Capacidad util | Pendiente de confirmar |
| `AporCaudal` | Rio | Diaria | Aporte hidrico observado | Pendiente de confirmar |
| `AporCaudalMediHist` | Rio | Diaria | Referencia historica de aporte | Pendiente de confirmar |
| `PorcApor` | Rio | Diaria | Indicador relativo de aporte | Pendiente de confirmar |
| `Gene` | Recurso | Horaria | Generacion real por recurso | Pendiente de confirmar |
| `PrecBolsNaci` | Sistema | Horaria | Precio de bolsa nacional | Pendiente de confirmar |

## 3. Catalogo CURATED de recursos

| Campo | Descripcion |
|---|---|
| `codigo` | Codigo unico usado para relacionar el recurso con `Gene.entidad`. |
| `nombre_referencia` | Nombre seleccionado de forma deterministica para uso tecnico. No implica que sea la variante semanticamente correcta cuando existe conflicto. |
| `nombres_catalogo` | Conjunto de variantes de nombre observadas para el codigo. |
| `tipo` | Tipo de recurso cuando existe un valor unico para el codigo. |
| `fuente_energia` | Fuente de energia informada por el catalogo. |
| `codigo_empresa` | Codigo de empresa asociado. |
| `estado` | Estado informado por el catalogo vigente. |
| `despacho` | Clasificacion de despacho informada. |
| `tipo_recurso` | Tipo adicional de recurso informado por XM. |
| `fecha_operacion` | Fecha de inicio de operacion informada. |
| `fecha_catalogo` | Fecha de la fotografia del catalogo utilizada. |
| `cantidad_registros_catalogo` | Numero de filas RAW encontradas para el codigo. |
| `duplicado_codigo` | Indica si el codigo aparecia mas de una vez en RAW. |
| `conflicto_nombre` | Indica si el mismo codigo tenia mas de un nombre. |
| `conflicto_otros_atributos` | Indica conflicto en otros atributos tecnicos evaluados. |

## 4. Generacion enriquecida

Ruta:

```text
/proyecto_xm/curated/generacion_enriquecida
```

| Campo | Descripcion |
|---|---|
| `metrica` | Metrica de origen (`Gene`). |
| `fecha` | Fecha de generacion. |
| `hora` | Hora 1-24. |
| `codigo_recurso` | Codigo del recurso procedente de `Gene.entidad`. |
| `generacion` | Valor numerico de generacion. Unidad pendiente de confirmar documentalmente. |
| `archivo_origen` | Archivo RAW de procedencia. |
| `year` | Particion anual. |
| `nombre_recurso` | Nombre de referencia del catalogo, si existe correspondencia. |
| `tipo` | Tipo de recurso del catalogo. |
| `fuente_energia` | Fuente energetica del catalogo. |
| `codigo_empresa` | Codigo de empresa. |
| `estado` | Estado del catalogo vigente. No debe usarse como estado historico sin validacion adicional. |
| `despacho` | Clasificacion de despacho vigente. |
| `tipo_recurso` | Clasificacion adicional. |
| `fecha_operacion` | Fecha de inicio de operacion informada. |
| `correspondencia_catalogo` | `true` si el codigo encontro correspondencia; `false` en caso contrario. |
| `es_hidraulico` | `true` cuando `tipo=HIDRAULICA` y `fuente_energia=AGUA`; `null` cuando no existe correspondencia de catalogo. |

## 5. Campos de control de calidad

En datasets rechazados pueden encontrarse los campos normalizados junto con:

| Campo | Descripcion |
|---|---|
| `valor_raw` | Valor original antes de conversion numerica. |
| `fecha_raw` | Fecha original antes de conversion. |
| `entidad_raw` | Entidad original antes de normalizacion. |
| `error_calidad` | Regla o combinacion de reglas incumplidas. |

Reglas implementadas:

- `R1_COMPLETITUD`
- `R2_INTEGRIDAD_NUMERICA`
- `R3_RANGO`
- `R4_DUPLICADO`

## 6. Notas de interpretacion

- El catalogo de recursos es una fotografia vigente y no debe tratarse automaticamente como dimension historica.
- Las asociaciones entre condicion hidrica, generacion y precio se analizaran como relaciones observadas, no como causalidad demostrada.
- Las unidades deben completarse solo cuando se validen contra documentacion oficial de XM.
