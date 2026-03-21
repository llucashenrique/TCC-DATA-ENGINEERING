import os
import re
import logging
from functools import reduce
from argparse import ArgumentParser

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


def setup_parser() -> ArgumentParser:
    """
    Explicação:
        Cria e configura um parser de argumentos de linha de comando para
        executar a migração de dados entre buckets do Google Cloud Storage
        e/ou para tabelas no BigQuery. O parser centraliza a validação dos
        parâmetros de entrada e define opções de conversão e simulação
        de execução.

    Argumentos:
        --from-bucket (str):
            Nome do bucket de origem, sem o prefixo 'gs://'.
            O valor é validado pela função validate_bucket_format e deve
            conter uma das camadas esperadas: landing, raw, staging ou curated.
            (obrigatório)

        --data-relative-path (str):
            Caminho relativo do arquivo dentro do bucket de origem,
            por exemplo: company/project/folder/arquivo.avro.
            O valor é validado pela função validate_data_relative_path_format.
            (obrigatório)

        --to-bucket (str):
            Nome do bucket de destino, sem o prefixo 'gs://'.
            Validado por validate_bucket_format.
            Necessário quando a opção --convert-to-parquet é utilizada.
            (opcional)

        --to-bq-table (str):
            Identificador da tabela de destino no BigQuery no formato
            project_id.dataset_id.table_id.
            O valor é validado por validate_bq_table_format.
            (opcional)

        --convert-to-parquet (bool):
            Quando informado, indica que o arquivo AVRO de entrada deve
            ser convertido para o formato Parquet no bucket de destino.
            Requer que o argumento --to-bucket seja fornecido.
            (opcional)

        --dry-run (bool):
            Executa o processo em modo de simulação, sem realizar qualquer
            escrita em buckets ou no BigQuery.
            (opcional)

    Exceções:
        argparse.ArgumentTypeError:
            Lançada pelas funções de validação caso algum argumento não
            esteja no formato esperado.

        ValueError:
            Pode ser lançada durante validações adicionais ou regras
            de negócio associadas aos argumentos fornecidos.

    Retorna:
        ArgumentParser:
            Instância do parser de argumentos configurado e pronto para
            processar os parâmetros de linha de comando.
    """

    parser = ArgumentParser(
        "Parser utilizado para migração de dados entre Buckets e BigQuery."
    )

    parser.add_argument(
        "--from-bucket",
        type=validate_bucket_format,
        required=True,
        help="Nome do bucket de origem (sem gs://). Deve conter landing/raw/staging/curated no nome.",
    )
    parser.add_argument(
        "--data-relative-path",
        type=validate_data_relative_path_format,
        required=True,
        help="Caminho relativo do arquivo dentro do bucket (ex: company/project/folder/arquivo.avro).",
    )
    parser.add_argument(
        "--to-bucket",
        type=validate_bucket_format,
        required=False,
        default=None,
        help="Nome do bucket de destino (sem gs://).",
    )
    parser.add_argument(
        "--to-bq-table",
        type=validate_bq_table_format,
        required=False,
        default=None,
        help="Destino no BigQuery: project_id.dataset_id.table_id",
    )
    parser.add_argument(
        "--convert-to-parquet",
        action="store_true",
        default=False,
        help="Converte AVRO de entrada para Parquet no destino (requer --to-bucket).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        required=False,
        help="Executa em modo de simulação (sem escrita).",
    )

    return parser


def validate_bucket_format(value: str) -> str:
    """
    Explicação:
        Valida se o nome do bucket informado está no formato esperado para uso
        na aplicação. A validação garante que o valor não contenha o prefixo
        'gs://' e que o nome do bucket indique uma das zonas/camadas de dados
        aceitas (landing, raw, staging, curated).

    Argumentos:
        value (str):
            Nome do bucket a ser validado, sem o prefixo 'gs://'.
            Exemplo válido: "meu-bucket-raw-zone".

    Exceções:
        ValueError:
            - Quando o valor informado começa com "gs://", indicando que foi
              fornecida uma URI completa em vez de apenas o nome do bucket.
            - Quando o nome do bucket não contém nenhuma das zonas esperadas:
              "landing", "raw", "staging" ou "curated".

    Retorna:
        str:
            O próprio valor (value) caso seja considerado válido.
    """

    if value.startswith("gs://"):
        raise ValueError(
            "Informe apenas o nome do bucket, sem 'gs://'. Ex: meu-bucket-raw-zone"
        )

    if not any(zone in value for zone in ["landing", "raw", "staging", "curated"]):
        raise ValueError(
            "Bucket deve conter uma das zonas: landing, raw, staging, curated."
        )
    return value


def validate_bq_table_format(value: str) -> str:
    """
    Explicação:
        Valida se o identificador de tabela do BigQuery informado segue o
        formato padrão de referência completa: "project_id.dataset_id.table_name".
        A validação é feita por meio de uma expressão regular que exige três
        segmentos separados por ponto, contendo apenas letras, números,
        underscore (_) e hífen (-).

    Argumentos:
        value (str):
            Identificador da tabela no BigQuery a ser validado.
            Deve seguir o formato: "project_id.dataset_id.table_name".
            Exemplo válido: "meu-projeto.meu_dataset.minha_tabela".

    Exceções:
        ValueError:
            Quando o valor não corresponde ao padrão esperado
            "project_id.dataset_id.table_name", contendo exatamente três partes
            separadas por '.', ou quando alguma parte possui caracteres fora de
            [A-Za-z0-9_-].

    Retorna:
        str:
            O próprio valor (value) caso esteja no formato válido.
    """

    if not re.match(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$", value):
        raise ValueError(
            "Tabela deve seguir o padrão 'project_id.dataset_id.table_name'"
        )
    return value


def validate_data_relative_path_format(value: str) -> str:
    """
    Explicação:
        Valida se o caminho relativo do arquivo dentro do bucket segue o
        padrão esperado pela aplicação. O caminho deve possuir exatamente
        quatro níveis hierárquicos separados por '/', representando
        organização lógica dos dados, e terminar com um arquivo nos formatos
        AVRO ou Parquet.

    Argumentos:
        value (str):
            Caminho relativo do arquivo dentro do bucket.
            Deve seguir o formato:
            "company/project/data_folder/arquivo.{avro|parquet}".
            Exemplos válidos:
            - "empresa1/projetoA/dados/arquivo.avro"
            - "company_x/project_y/folder_z/data.parquet"

    Exceções:
        ValueError:
            Quando o caminho informado não possui exatamente quatro níveis
            separados por '/', ou quando o nome do arquivo não termina com
            as extensões permitidas (.avro ou .parquet), ou ainda quando
            contém caracteres fora do conjunto permitido
            [A-Za-z0-9_-].

    Retorna:
        str:
            O próprio valor (value) caso esteja em conformidade com o
            formato esperado.
    """

    if not re.match(
        r"^[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+\.(avro|parquet)$",
        value,
    ):
        raise ValueError(
            "Caminho relativo deve seguir: 'company/project/data_folder/arquivo.{avro|parquet}'."
        )
    return value


def setup_logger() -> logging.Logger:
    """
    Explicação:
        Configura o sistema de logging padrão da aplicação e retorna um logger
        identificado pelo nome do arquivo atual. O logger é configurado para
        exibir mensagens no nível INFO ou superior, com um formato padronizado
        que inclui data, nível do log, nome do logger e a mensagem.

    Argumentos:
        Nenhum.
        A função não recebe parâmetros e utiliza configurações padrão do
        módulo logging.

    Exceções:
        Nenhuma explicitamente.
        Eventuais exceções podem ocorrer apenas em cenários atípicos, como
        problemas no acesso ao valor de __file__ em ambientes muito
        restritos, mas não são tratadas diretamente pela função.

    Retorna:
        logging.Logger:
            Instância de logger configurada com o nome do arquivo atual
            (obtido via os.path.basename(__file__)), pronta para ser usada
            para registrar mensagens de log na aplicação.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%y/%m/%d %H:%M:%S",
    )
    logger_name = os.path.basename(_file_)
    return logging.getLogger(logger_name)


def setup_spark() -> SparkSession:
    """
    Explicação:
        Cria e configura uma sessão do Apache Spark para execução em ambiente
        de cluster utilizando o gerenciador de recursos YARN. A função aplica
        configurações específicas para escrita de arquivos Parquet e ajusta
        o nível de log do Spark para reduzir verbosidade durante a execução.

    Argumentos:
        Nenhum.
        A função não recebe parâmetros e utiliza configurações padrão
        para inicialização da SparkSession.

    Exceções:
        Exception:
            Pode lançar exceções relacionadas à inicialização da SparkSession,
            como falha na conexão com o cluster YARN, configurações inválidas
            de ambiente ou indisponibilidade do Spark.

    Retorna:
        SparkSession:
            Instância ativa de SparkSession configurada para execução em
            cluster via YARN, com ajustes aplicados para escrita de Parquet
            e nível de log definido como FATAL.
    """

    spark_session = SparkSession.builder.master("yarn").getOrCreate()
    # Para evitar problemas ao substituir datas vazias por 0001-01-01
    spark_session.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    # Para loggar apenas erros fatais
    spark_session.sparkContext.setLogLevel("FATAL")

    return spark_session


def read_data(
    spark_session: SparkSession, source_path: str, schema: StructType | None = None
) -> DataFrame:
    """
    Explicação:
        Lê um arquivo de dados a partir de um caminho (ex.: GCS/HDFS/local),
        utilizando uma SparkSession, e retorna um DataFrame Spark validado.
        O formato de leitura é inferido pela extensão do arquivo, suportando
        atualmente AVRO e Parquet. Opcionalmente, permite aplicar um schema
        explícito antes da leitura. Após carregar os dados, a função executa
        validate_dataframe(df) para garantir que o DataFrame resultante
        atende aos critérios mínimos definidos pela aplicação.

    Argumentos:
        spark_session (SparkSession):
            Sessão Spark utilizada para realizar a leitura dos dados.

        source_path (str):
            Caminho completo do arquivo a ser lido. A extensão do arquivo
            (ex.: ".avro" ou ".parquet") determina o leitor utilizado.

        schema (StructType | None):
            Schema opcional a ser aplicado durante a leitura. Quando fornecido,
            a leitura é feita com reader.schema(schema) para impor tipos e
            estrutura. Quando None, o Spark infere o schema (quando aplicável).
            (opcional)

    Exceções:
        ValueError:
            Quando a extensão do arquivo não é suportada para leitura
            (qualquer valor diferente de "avro" ou "parquet").

        FileNotFoundError:
            Quando ocorre uma AnalysisException durante a leitura (por
            exemplo, arquivo inexistente, caminho incorreto ou problemas de
            acesso). A exceção original é encadeada via from e.

        Exception:
            Outras exceções podem ser propagadas indiretamente, como erros de
            permissões, falhas de I/O, inconsistências de schema ou problemas
            no ambiente Spark.

    Retorna:
        DataFrame:
            DataFrame Spark carregado a partir do source_path e validado
            pela função validate_dataframe.
    """

    data_extension = source_path.split(".")[-1].lower()

    try:
        if data_extension == "avro":
            reader = spark_session.read.format("avro")
            if schema is not None:
                reader = reader.schema(schema)
            df = reader.load(source_path)

        elif data_extension == "parquet":
            reader = spark_session.read
            if schema is not None:
                reader = reader.schema(schema)
            df = reader.parquet(source_path)

        else:
            raise ValueError(f"Formato '{data_extension}' não suportado para leitura.")

        validate_dataframe(df)
        return df

    except AnalysisException as e:
        raise FileNotFoundError(
            "Erro ao ler o arquivo. Verifique se existe e se o caminho está correto."
        ) from e


def validate_dataframe(df: DataFrame) -> None:
    """
    Explicação:
        Valida se um DataFrame Spark contém dados utilizáveis para processamento.
        A função garante que:
        - o DataFrame pode ser lido corretamente (schema válido),
        - não está vazio,
        - possui ao menos uma linha com algum valor não nulo e não vazio
          em qualquer coluna.

        A validação considera como "vazio" valores nulos, strings vazias
        ou strings contendo apenas espaços em branco.

    Argumentos:
        df (DataFrame):
            DataFrame Spark a ser validado antes de etapas posteriores
            de processamento ou gravação.

    Exceções:
        ValueError:
            - Quando ocorre um erro ao acessar os dados do DataFrame,
              geralmente causado por problemas de schema ou leitura
              (Py4JJavaError).
            - Quando o DataFrame está completamente vazio (sem linhas).
            - Quando o DataFrame contém apenas linhas com valores nulos
              ou vazios em todas as colunas.

        Py4JJavaError:
            Capturada internamente durante a tentativa de leitura do
            DataFrame (df.take(1)), sendo encapsulada e relançada como
            ValueError com uma mensagem mais descritiva.

    Retorna:
        None:
            A função não retorna nenhum valor. Caso nenhuma exceção seja
            lançada, o DataFrame é considerado válido para uso.
    """

    try:
        first = df.take(1)
    except Py4JJavaError as e:
        raise ValueError(
            "Erro ao ler o arquivo. Verifique se o schema está correto."
        ) from e

    if not first:
        raise ValueError("DataFrame está vazio.")

    empty_cols = [
        (F.coalesce(F.trim(F.col(c).cast("string")), F.lit("")) == F.lit("")).alias(c)
        for c in df.columns
    ]

    flags = df.select(*empty_cols)

    all_empty_row = reduce(lambda a, b: a & b, [F.col(c) for c in flags.columns])
    has_non_empty = flags.filter(~all_empty_row).limit(1).count() > 0

    if not has_non_empty:
        raise ValueError("DataFrame contém apenas linhas nulas ou vazias.")


def get_data_info(schema_ref: str) -> StructType:
    """
    Explicação:
        Retorna um schema (StructType) do Spark com base em uma referência
        textual (schema_ref). Essa função centraliza o mapeamento entre nomes
        lógicos de schemas utilizados pela aplicação e suas definições formais
        (lista de StructField), permitindo reutilização e padronização na
        leitura/validação de dados.

        Atualmente, o schema reconhecido é:
        - "SCD_ON_TIME_DELIVERY_GOALS"

    Argumentos:
        schema_ref (str):
            Identificador do schema desejado. Deve corresponder a uma referência
            conhecida pelo módulo (ex.: "SCD_ON_TIME_DELIVERY_GOALS").

    Exceções:
        ValueError:
            Quando schema_ref não corresponde a nenhum schema reconhecido
            pela função.

    Retorna:
        StructType:
            Objeto StructType do PySpark representando o schema associado
            ao schema_ref informado, contendo campos e tipos definidos.
    """

    if schema_ref == "SCD_ON_TIME_DELIVERY_GOALS":
        return StructType(
            [
                StructField("ID", StringType(), True),
                StructField("TECHNOLOGY", StringType(), True),
                StructField("GOAL", StringType(), True),
                StructField("YEAR", StringType(), True),
                StructField("UPDATE_DATE", StringType(), True),
            ]
        )

    raise ValueError(f"Schema '{schema_ref}' não reconhecido.")


def write_data(df: DataFrame, target_path: str) -> None:
    """
    Explicação:
        Escreve um DataFrame Spark no caminho de destino (target_path),
        inferindo o formato de escrita pela extensão do arquivo no caminho.
        Atualmente, os formatos suportados são Parquet e Avro.

        - Para Parquet, a escrita é feita em modo overwrite, com schema
          sobrescrito (overwriteSchema=true) e compressão Snappy.
        - Para Avro, a escrita é feita em modo overwrite utilizando o
          writer no formato "avro".

    Argumentos:
        df (DataFrame):
            DataFrame Spark contendo os dados a serem persistidos no destino.

        target_path (str):
            Caminho completo de destino onde os dados serão gravados.
            A extensão do caminho define o formato:
            - termina com ".parquet" para escrita em Parquet
            - termina com ".avro" para escrita em Avro

    Exceções:
        ValueError:
            Quando a extensão extraída de target_path não é suportada
            para escrita (qualquer valor diferente de "parquet" ou "avro").

        Exception:
            Outras exceções podem ser propagadas pelo Spark durante a escrita,
            como problemas de permissão, falha de I/O, inconsistência de schema
            ou indisponibilidade do filesystem/serviço de armazenamento.

    Retorna:
        None:
            A função não retorna nenhum valor. Caso nenhuma exceção seja
            lançada, a escrita é considerada concluída com sucesso.
    """

    ext = target_path.split(".")[-1].lower()

    if ext == "csv":
        (
            df.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .option("compression", "snappy")
            .parquet(target_path)
        )
    elif ext == "parquet":
        (df.write.mode("overwrite").format("parquet").save(target_path))
    else:
        raise ValueError(f"Formato '{ext}' não suportado para escrita.")


def convert_avro_to_parquet(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    schema: StructType | None = None,
) -> str:
    """
    Explicação:
        Converte um arquivo no formato AVRO para o formato Parquet utilizando
        Apache Spark. A função valida as extensões dos caminhos de origem e
        destino, lê os dados com read_data() (opcionalmente aplicando um
        schema explícito) e grava o resultado em Parquet com write_data().

        Essa função é útil para padronização de formatos de dados em pipelines,
        permitindo transformar AVRO em Parquet de forma consistente e validada.

    Argumentos:
        spark (SparkSession):
            Sessão do Spark utilizada para executar a leitura e a escrita
            dos dados.

        source_path (str):
            Caminho completo do arquivo de origem. Deve terminar com ".avro".
            Exemplo: "gs://meu-bucket/caminho/arquivo.avro".

        target_path (str):
            Caminho completo do destino. Deve terminar com ".parquet".
            Exemplo: "gs://meu-bucket/caminho/arquivo.parquet".

        schema (StructType | None):
            Schema opcional a ser aplicado durante a leitura do AVRO.
            Quando fornecido, é repassado para read_data() para impor a
            estrutura e os tipos das colunas. Quando None, o Spark infere o
            schema (quando aplicável).
            (opcional)

    Exceções:
        ValueError:
            - Quando source_path não termina com ".avro".
            - Quando target_path não termina com ".parquet".
            - Pode também ser propagada por read_data()/write_data() em casos
              de formato inválido ou validações internas (ex.: DataFrame vazio).

        FileNotFoundError:
            Pode ser propagada por read_data() caso ocorra erro de leitura
            relacionado a caminho inexistente/incorreto (ex.: encapsulando
            AnalysisException).

        Exception:
            Outras exceções podem ser propagadas pelo Spark durante leitura
            ou escrita (permissões, I/O, inconsistências de schema, etc.).

    Retorna:
        str:
            O próprio target_path, indicando o caminho de destino onde os dados
            foram gravados em Parquet (caso a operação seja concluída sem erro).
    """

    if not source_path.lower().endswith(".avro"):
        raise ValueError("convert_avro_to_parquet: source_path deve terminar com .avro")

    if not target_path.lower().endswith(".parquet"):
        raise ValueError(
            "convert_avro_to_parquet: target_path deve terminar com .parquet"
        )

    df = read_data(spark, source_path, schema=schema)
    write_data(df, target_path)
    return target_path


def write_to_bq(df: DataFrame, table_ref: str) -> None:
    """
    Explicação:
        Escreve um DataFrame Spark em uma tabela do BigQuery utilizando o
        conector Spark BigQuery. A escrita é realizada em modo overwrite,
        substituindo os dados existentes na tabela de destino, e utiliza
        o método de escrita direta ("direct") para envio dos dados.

    Argumentos:
        df (DataFrame):
            DataFrame Spark contendo os dados que serão gravados no BigQuery.

        table_ref (str):
            Referência da tabela de destino no BigQuery no formato
            "project_id.dataset_id.table_id".

    Exceções:
        Exception:
            Exceções podem ser lançadas durante a escrita no BigQuery, como
            erros de autenticação, permissões insuficientes, tabela inexistente,
            problemas de schema incompatível ou falhas de comunicação com o
            serviço do BigQuery.

    Retorna:
        None:
            A função não retorna nenhum valor. Caso nenhuma exceção seja
            lançada, os dados são gravados com sucesso na tabela do BigQuery.
    """

    df.write.format("bigquery").option("table", table_ref).option(
        "writeMethod", "direct"
    ).mode("overwrite").save()


def main() -> None:
    """
    Explicação:
        Função principal (entrypoint) do job de migração de dados. Ela:
        1) Inicializa o logger.
        2) Configura e processa os argumentos de linha de comando.
        3) Valida regras de negócio entre argumentos (destino obrigatório e
           dependência do --convert-to-parquet com --to-bucket).
        4) Deriva a referência de schema a partir do nome do arquivo no
           data_relative_path e obtém o schema via get_data_info.
        5) Monta as URIs de origem e destino no GCS (gs://...).
        6) Se --dry-run estiver habilitado, apenas registra o que seria feito
           e encerra sem realizar leituras/escritas.
        7) Caso contrário, inicializa uma SparkSession via YARN, lê os dados
           (AVRO/Parquet) com schema opcional e:
            - Converte AVRO -> Parquet no GCS quando --convert-to-parquet está ativo; ou
            - Escreve no GCS mantendo o formato original quando --to-bucket é fornecido; e/ou
            - Escreve no BigQuery quando --to-bq-table é fornecido.
        8) Finaliza garantindo o encerramento da sessão Spark no bloco finally.

        Observações importantes:
        - O schema_ref é inferido como o nome do arquivo sem extensão:
          schema_ref = data_relative_path.split("/")[-1].split(".")[0].
          Assim, o arquivo deve estar nomeado de forma que corresponda a uma
          referência reconhecida por get_data_info.
        - Quando --convert-to-parquet está habilitado, o destino no bucket é
          gerado substituindo a extensão final por ".parquet".

    Argumentos:
        Nenhum.
        Os parâmetros são recebidos via linha de comando e interpretados
        por argparse (por exemplo: --from-bucket, --data-relative-path,
        --to-bucket, --to-bq-table, --convert-to-parquet, --dry-run).

    Exceções:
        SystemExit:
            Pode ser levantada indiretamente pelo argparse quando ocorre
            erro de parsing/validação de argumentos (ex.: parser.error(...)).

        ValueError:
            - Quando --convert-to-parquet é usado com arquivo de entrada que
              não termina com ".avro".
            - Quando get_data_info(schema_ref) não reconhece a referência
              de schema inferida a partir do nome do arquivo.

        FileNotFoundError:
            Pode ser propagada por read_data() quando ocorre falha de leitura
            (ex.: caminho inexistente ou inacessível), encapsulando erros do Spark.

        Exception:
            Outras exceções podem ocorrer durante inicialização do Spark,
            leitura, transformação, escrita em GCS/BigQuery ou por questões
            de permissões, conectividade e inconsistência de schema.

    Retorna:
        None:
            Não retorna nenhum valor. Em caso de execução bem-sucedida, o job
            finaliza após realizar as operações solicitadas; em --dry-run,
            finaliza após registrar o plano de execução.
    """

    logger = setup_logger()
    parser = setup_parser()
    args = parser.parse_args()

    source_bucket = args.from_bucket
    target_bucket = args.to_bucket
    target_bq = args.to_bq_table
    data_relative_path = args.data_relative_path
    convert_to_parquet = args.convert_to_parquet
    dry_run = args.dry_run

    if convert_to_parquet and target_bucket is None:
        parser.error("--convert-to-parquet requer --to-bucket (destino no GCS).")

    if target_bucket is None and target_bq is None:
        parser.error("Informe pelo menos um destino: --to-bucket e/ou --to-bq-table.")

    schema_ref = data_relative_path.split("/")[-1].split(".")[0]
    schema = get_data_info(schema_ref)

    source_uri = f"gs://{source_bucket}/{data_relative_path}"

    target_uri = None
    if target_bucket is not None:
        target_data_relative_path = (
            re.sub(r"\.[a-zA-Z]+$", ".parquet", data_relative_path, count=1)
            if convert_to_parquet
            else data_relative_path
        )
        target_uri = f"gs://{target_bucket}/{target_data_relative_path}"

    logger.info("Iniciando job...")
    if dry_run:
        logger.warning("Modo dry-run selecionado. Nenhuma escrita será realizada.")
        logger.info(f"Dados seriam lidos de: {source_uri}")
        logger.info(f"Schema ref: {schema_ref}")

        if convert_to_parquet:
            logger.info("Conversão AVRO->PARQUET estaria ativa.")
            if target_uri:
                logger.info(f"Dados seriam convertidos e escritos em: {target_uri}")

        else:
            if target_uri:
                logger.info(f"Dados seriam escritos (mesmo formato) em: {target_uri}")

        if target_bq:
            logger.info(f"Dados seriam escritos no BigQuery em: {target_bq}")

        logger.info("Dry-run finalizado.")
        return

    logger.info("Configurando Spark...")
    spark = setup_spark()

    try:
        logger.info(f"Lendo dados de {source_uri}...")
        df = read_data(spark, source_uri, schema)

        if convert_to_parquet:
            if not source_uri.lower().endswith(".avro"):
                raise ValueError(
                    "--convert-to-parquet só pode ser usado quando o input for .avro"
                )

            logger.info(f"Convertendo AVRO->PARQUET e escrevendo em {target_uri}...")
            convert_avro_to_parquet(
                spark=spark,
                source_path=source_uri,
                target_path=target_uri,
                schema=schema,
            )

        elif target_uri is not None:
            logger.info(f"Salvando dados no GCS em {target_uri}...")
            write_data(df, target_uri)

        if target_bq is not None:
            logger.info(f"Salvando dados no BQ em {target_bq}...")
            write_to_bq(df, target_bq)

        logger.info("Job finalizado com sucesso!")

    finally:
        logger.info("Interrompendo Spark...")
        spark.stop()


if _name_ == "_main_":
    main()
    
Ler do MinIO

Padronizar minimamente

Converter formatos (ex: CSV → Parquet)

Adicionar metadados (ingestion_date)