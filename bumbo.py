from pyspark.sql import functions as f

class ExceptionTableNotLoad(Exception):
    pass

class Entity:
    def __init__(self, name, *columns, joins=None):
        self.tables = {}
        self.columns = columns
        for column in columns:
            setattr(self, column.name, column)
        self.joins = joins or []
        self.df = None

    def load(self, table):
        self.tables[table.name] = table

    def _set_columns(self, df):
        _df = df
        for column in self.columns:
            _df = _df.withColumn(column.name, column.transform.cast(column.dtype))
        return _df

    def _join(self, df):
        _df = df
        for join in self.joins:
            _df = join.make_join(df=_df, how="left")

        return _df

    def _select(self, df):
        cols = [f.col(column.name).cast(column.dtype) for column in self.columns]
        return df.select(*cols)

    def transform(self, table):
        self.load(table)
        df = table.df
        df = self._set_columns(df)
        self.df = self._select(df)
        return self.df

    def to_s3(self, location):
        pass
    
    def to_table(self, table_name):
        pass

class Column:
    def __init__(self, name, transform, dtype, nullable=True, unique=False, length=None):
        self.name = name
        self.transform = transform
        self.dtype = dtype

class Table:
    def __init__(self, name, location, keys=None, joins=None):
        selfleft_df.name = name
        self.location = location
        self.joins = joins
        self._df = None

    @property
    def df(self):
        if self._df is None:
            raise ExceptionTableNotLoad("Use table.load(...)")
        return self._df
    
    def load(self, format='csv', sep="|", infer_schema=True, header=True, **options):
        if self._df is None:
            self._df = spark.read.load(
                path=self.location,
                format=format,
                sep=sep,
                inferSchema=infer_schema,
                header=header,
                **options)

class Join:
    def __init__(self, left, right, on):
        self.on = on
        self.left = left
        self.right = right

    def make_join(self, how, df=None):
        left_df = self.left.df if df is None else df
        rdf = left_df.join(
            self.right.df,
            self.on,
            how
        )
        drop_cols = [col for col in self.right.df.columns if col in left_df.columns]
        for col in drop_cols:
            rdf = rdf.drop(self.right.df[col])
        return rdf