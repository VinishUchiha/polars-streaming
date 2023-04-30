<div align="center">

# polars-streaming

<div align="left">

This library helps to process streaming data using Polars.

## Installation
```bash
pip install polars-streaming
```
**Install from sources**

Alternatively, you can also clone the latest version from the [repository](https://github.com/VinishUchiha/polars-streaming) and install it directly from the source code:

```bash
pip install -e .
```

## Quick tour
```python
>>> from polars_streaming import StreamProcessor

>>> s = StreamProcessor()
>>> s.readStream.format('socket').options({'host':'localhost','port':12345}).load()

>>> def transformation(df):
>>>     return df.tail() # Add your transformation code here

>>> s.add_transform(transformation)
>>> s.writeStream.format('console').trigger('3 seconds')

>>> s.start()
```
