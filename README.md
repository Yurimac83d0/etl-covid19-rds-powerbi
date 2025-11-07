# ETL COVID-19 Brazil → PostgreSQL RDS → Power BI

## Descrição
Este projeto realiza um pipeline **ETL (Extract, Transform, Load)** para dados de COVID-19 no Brasil.  
Ele baixa os dados, transforma e valida, grava em um **banco PostgreSQL na AWS RDS** e gera gráficos para visualização.  
Os dados podem ser visualizados diretamente no **Power BI** para análise de evolução da pandemia.

---

## Funcionalidades
- Baixa dados do GitHub e do Brasil.io (fallback)  
- Filtra apenas os dados do Brasil  
- Converte tipos de dados e calcula métricas, como casos por 100 mil habitantes  
- Valida inconsistências (datas faltantes, valores negativos)  
- Salva CSVs locais e no banco PostgreSQL RDS  
- Gera gráficos da evolução de casos  
- Logging detalhado de todas as etapas

---

## Tecnologias
- **Python 3**  
- **Pandas, Numpy** → manipulação de dados  
- **Requests** → download de arquivos CSV  
- **SQLAlchemy, Psycopg2** → conexão e gravação no PostgreSQL  
- **Matplotlib** → gráficos  
- **PostgreSQL RDS AWS** → banco de dados na nuvem  
- **Power BI** → visualização de dados

---

## Como rodar
1. Configure as variáveis de ambiente para o RDS:

```bash
export PG_USER=postgres
export PG_PASSWORD='Xamp208s'
export PG_HOST=database-1.cuu84l0.sa-east-1.rds.amazonaws.com
export PG_PORT=5432
export PG_DBNAME=postgres

2. Instale dependências:

pip3 install pandas numpy requests sqlalchemy psycopg2-binary matplotlib


3. Execute o script ETL:

python3 etl.py


4. Conecte o Power BI ao PostgreSQL usando o endpoint RDS para criar dashboards interativos.
