import time
import pandas as pd
import polars as pl
import modin.pandas as mpd
from memory_profiler import memory_usage

# Caminho para o dataset
dataset_path = "Dataset\\bankdataset.xlsx"

# Função para medir desempenho
def measure_performance(func, *args, **kwargs):
    start_time = time.time()
    mem_usage = memory_usage((func, args, kwargs), interval=0.1, retval=True)
    end_time = time.time()
    elapsed_time = end_time - start_time
    max_mem = max(mem_usage[0])  # Pega o maior uso de memória
    result = mem_usage[1]       # Resultado da função executada
    return elapsed_time, max_mem, result

# Funções para carregar com cada biblioteca
def load_with_pandas(file_path):
    df = pd.read_excel(file_path)
    df['Value_Transaction'] = df['Value'] * df['Transaction_count']  # Nova coluna
    return df

def load_with_polars(file_path):
    df = pl.read_excel(file_path)
    df = df.with_columns((df['Value'] * df['Transaction_count']).alias('Value_Transaction'))  # Nova coluna
    return df

def load_with_modin(file_path):
    df = mpd.read_excel(file_path)
    df['Value_Transaction'] = df['Value'] * df['Transaction_count']  # Nova coluna
    return df

def load_with_dask(file_path):
    import dask.dataframe as dd
    df = dd.read_excel(file_path)
    df['Value_Transaction'] = df['Value'] * df['Transaction_count']
    df.compute()  # Computa os resultados
    return df

def load_with_vaex(file_path):
    import vaex
    df = vaex.open(file_path)
    df['Value_Transaction'] = df['Value'] * df['Transaction_count']
    return df

def load_with_koalas(file_path):
    import pyspark.pandas as ps  # Usando pandas-on-Spark
    df = ps.read_excel(file_path, sheet_name=0)  # Carregando o Excel com pandas-on-Spark
    df['Value_Transaction'] = df['Value'] * df['Transaction_count']
    return df

# Comparação de desempenho
def run_comparison():
    print("Comparando bibliotecas Pandas, Polars e Modin...")
    
    # # Teste com Pandas
    # print("Testando com Pandas...")
    # pandas_time, pandas_mem, _ = measure_performance(load_with_pandas, dataset_path)
    # print(f"Pandas - Tempo de Load + Nova Coluna: {pandas_time:.2f}s | Memória: {pandas_mem:.2f} MiB")
    
    # # Teste com Polars
    # print("Testando com Polars...")
    # polars_time, polars_mem, _ = measure_performance(load_with_polars, dataset_path)
    # print(f"Polars - Tempo de Load + Nova Coluna: {polars_time:.2f}s | Memória: {polars_mem:.2f} MiB")
    
    # # Teste com Modin
    # print("Testando com Modin...")
    # modin_time, modin_mem, _ = measure_performance(load_with_modin, dataset_path)
    # print(f"Modin - Tempo de Load + Nova Coluna: {modin_time:.2f}s | Memória: {modin_mem:.2f} MiB")
    
    # # Teste com Dask
    # dask_time, dask_mem, _ = measure_performance(load_with_dask, dataset_path)
    # print(f"Dask - Tempo de Load + Nova Coluna: {dask_time:.2f}s | Memória: {dask_mem:.2f} MiB")

    # Teste com Vaex
    # vaex_time, vaex_mem, _ = measure_performance(load_with_vaex, dataset_path)
    # print(f"Vaex - Tempo de Load + Nova Coluna: {vaex_time:.2f}s | Memória: {vaex_mem:.2f} MiB")

    # Teste com Koalas
    print('Testando com Koalas...')
    koalas_time, koalas_mem, _ = measure_performance(load_with_koalas, dataset_path)
    print(f"Koalas - Tempo de Load + Nova Coluna: {koalas_time:.2f}s | Memória: {koalas_mem:.2f} MiB")

# Executar comparação
if __name__ == "__main__":
    run_comparison()


