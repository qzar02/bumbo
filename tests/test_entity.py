import unittest
from pyspark.sql import functions as f
from . import sparktest
from bumbo import Entity, Table, Column , Join, ExceptionTableNotLoad
transform_x = lambda col: None
sum_a_b = lambda a, b: f.col(a) + f.col(b)

class TestTable(sparktest.PySparkTest):

    def setUp(self):
        pass
    
    def test_load_raw_data_in_table(self):
        t = Table(name="t", keys=[], location='./tests/data/table_t.csv')
        t.load()
        self.assertEqual(t.df.dtypes, [("col1", "int"), ("col2", "string"), ("col3", "string"), ("col4", "double")])

    def test_table_not_load(self):
        t = Table(name="t", keys=[], location='./tests/data/table_t.csv')
        with self.assertRaises(ExceptionTableNotLoad) as context:
            x = t.df
        self.assertTrue('Use table.load(...)' in str(context.exception))

    def test_join_between_tables(self):
        a = Table(name="a", location='./tests/data/table_a.csv')
        t = Table(name="t", location='./tests/data/table_t.csv')

        join_with_a = Join(left=a, right=t, on=["col1"])
        
        a.load()
        t.load()

        df = join_with_a.make_join(how='left')
        self.assertEqual(join_with_a.left, a)
        self.assertEqual(join_with_a.right, t)
        self.assertEqual(['col1', 'col2', 'col3', 'col4'], df.columns)

class TestEntity(sparktest.PySparkTest):

    def setUp(self):
        pass
        # bumbo.spark = self.spark
        # table_x = bumbo.Table('table_x', ["COL1", "COL2"], location='s3://raw/data/table_x/')
        # table_a = Table('table_a', ["COLX", "COLY"], location='s3://raw/data/table_a/')

        # entity_teste = Entity(
        #     Column('COLUMN1', transform_x('COL1'), 'string'),
        #     Column('COLUMN2', transform_x('COL2'), reference=table_x),
        #     Column('COLUMN3', transform_x('COL3'), 'string'),
        #     Column('COLUMN4', transform_x('COL4'), 'string'),
        #     Column('COLUMN5', transform_x('COL5'), 'string'),
        #     Column('COLUMN6', transform_x('COL6'), 'string'),
        # )
        # entity_teste.load(table_x)
        # entity_teste.transform(table_a)
        # entity_teste.to_s3(location='s3://trusted/entity_teste')
        # entity_teste.to_table(table_name='TBL_TESTE')

    def test_set_column_in_entity(self):
        entity_a = Entity(
            'ENTITY_A',
            Column('COLUMN1', transform_x('COL1'), 'string'),
            Column('COLUMN_2', transform_x('COL2'), 'string'),
            Column('COLUMN3', transform_x('COL3'), 'string'),
            Column('COLUMN4', transform_x('COL4'), 'string'),
            Column('COLUMN5', transform_x('COL5'), 'string'),
            Column('COLUMN6', transform_x('COL6'), 'string'),
        )
        self.assertEqual(entity_a.COLUMN1.name, 'COLUMN1')
        self.assertEqual(entity_a.COLUMN_2.name, 'COLUMN_2')

    def test_load_raw_data(self):
        table_a = Table('table_a', keys=["COL1", "COL2"], location='s3://raw/data/table_a/')
        table_b = Table('table_b', keys=["COLX", "COLY"], location='s3://raw/data/table_b/')
        entity_b = Entity(
            'ENTITY_B',
            Column('COLUMN1', transform_x('COL1'), 'string'),
            Column('COLUMN2', transform_x('COL2'), 'string'),
            Column('COLUMN3', transform_x('COL3'), 'string'),
            Column('COLUMN4', transform_x('COL4'), 'string'),
            Column('COLUMN5', transform_x('COL5'), 'string'),
            Column('COLUMN6', transform_x('COL6'), 'string'),
        )
        entity_b.load(table_a)
        entity_b.load(table_b)
        self.assertEqual(len(entity_b.tables),2) 
        self.assertEqual(entity_b.tables['table_a'], table_a) 
        self.assertEqual(entity_b.tables['table_b'], table_b) 
    

    def test_set_columns_in_transform(self):
        table_a = Table(name="a", keys=[], location='./tests/data/table_a.csv')
        table_a.load()

        entity_b = Entity(
            'ENTITY_B',
            Column('COLUMN_SUM_A_B', sum_a_b('col1', 'col2'), 'int')
        )
        df = entity_b._set_columns(table_a.df)
        
        self.assertTrue('COLUMN_SUM_A_B' in df.columns)
        self.assertEqual(df.collect()[0].COLUMN_SUM_A_B, 3)
        self.assertEqual(df.collect()[2].COLUMN_SUM_A_B, None)
        self.assertTrue(('COLUMN_SUM_A_B', 'int') in df.dtypes)

    def test_select_columns_in_transform(self):
        table_a = Table(name="a", keys=[], location='./tests/data/table_a.csv')
        table_a.load()

        entity_b = Entity(
            'ENTITY_B',
            Column('COLUMN_SUM_A_B', sum_a_b('col1', 'col2'), 'int'),
            Column('col1', f.col('col1'), 'int')
        )

        df = entity_b._set_columns(table_a.df)
        df = entity_b._select(df)
        self.assertTrue('col1' in df.columns)
        self.assertTrue('col2' not in df.columns)

    def test_join_tables(self):
        a = Table(name="a", location='./tests/data/table_a.csv')
        a.load()
        t = Table(name="t", location='./tests/data/table_t.csv')
        t.load()
        ft = Table(name="fake_t", location='./tests/data/table_t.csv')
        ft.load()
        
        join_t_with_a = Join(left=a, right=t, on=["col1"])
        join_t_with_a_x = Join(left=a, right=t, on=["col1"])

        entity_b = Entity(
            'ENTITY_B',
            Column('COLUMN_SUM_A_B', sum_a_b('col1', 'col2'), 'int'),
            Column('col1', f.col('col1'), 'int'),
            joins=[join_t_with_a, join_t_with_a_x]
        )
        
        entity_b.load(a)
        entity_b.load(t)
        df = entity_b._join(a.df)
        self.assertEqual(['col1', 'col2', 'col3', 'col4'], df.columns)




if __name__ == '__main__':
    unittest.main()
    bumbo.spark.stop()