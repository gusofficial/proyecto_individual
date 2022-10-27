# Proyecto individual 1: ETL
El objetivo de este proyecto fue la limpieza y armado de una base de datos para la carga incremental de archivos.

## Datos
Se utilizaron los datos proporcionados en el siguiente [drive](https://drive.google.com/drive/folders/1Rsq-HHomPtQwy7RIWQ574wKcf56LiGq1).

## Base de datos
Se realizó la limpieza de los archivos que comprenden la base de datos: `sucursal` y `producto`. Luego se armó la base de datos, disponible en la carpeta `base de datos` con los datos normalizados.

## ETL
Para el ETL se utilizó python y se realizó la conexión a mysql con mysqlalchemy.

El ETL comprende la carga de archivo, la normalización y la carga a la tabla de precios de la base de datos. Un diagrama se encuentra a continuación:

<p align="center">
  <img src="figuras/Diagrama.png">
</p>
