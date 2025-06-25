# Docker
1. Inicie o Docker Desktop e suba apenas o elasticsearch e o kibana.
2. Acesse localhost:9200 e localhost:9200/_cat/indices para verificar se o elasticsearch está funcionando corretamente.

# importar
1. Acesse localhost:5601 para acessar a tela inicial do kibana.
2. Clique no link Upload a file no centro da tela
3. Selecione o arquivo a ser indexado, confirme os campos e clique em import
4. Digite um nome para o índice [queimadas, desmatamentos] e clique em import

# criar o dashboard (queimadas)
1. Acesse no menu lateral Analytics > Dashboard e clique no botão Create dashboard
2. Clique no botão Save no topo direito da página, vai abrir uma popup
3. Digite um título e do dashboard e depois clique em Salvar
     - Focos de Queimadas no Bioma Cerrado por Ano
4. Ao lado de Create Visualization clique em Create new text e digite o conteúdo do primeiro markdown, em seguida clique em Update

Estatísticas dos focos de incêndios no bioma Cerrado
 - fonte: https://terrabrasilis.dpi.inpe.br/queimadas/situacao-atual/estatisticas/estatisticas_estados/

Distribuição de Frequência: 
 - Agrupa os dados em intervalos (classes) e indica quantas ocorrências existem em cada intervalo

# Corrigir o ano/separador milhar (se necessário)
1. Management > Stack Management
2. Kibana > Index Patterns
3. Clique em editar no campo ano
4. Marque a opção Set Format
     Format: Number
     Numeral Format Pattern: 0
5. Clique em save
6. Clique em Management > Stack Management
7. Kibana > Advanced Settings
8. Mude o Formating Locale para Portuguese (Brazil)

# adicionar visualizações (queimadas)
Todas as visualizações são criadas indo em Analytics > Visualize Library
depois clicando no botão Create visualization e selecionando o índice desejado
Selecione em seguida Aggregation based e siga os próximos passos abaixo a partir daí.
Depois de criar cada um clique em Update para visualizar e Save para salvar, digite o título e selecione o dashboard, depois save.

1. Distribuição de Frequências Absolutas (Anos por focos)
Data Table > queimadas:
Add Bucket
     Aggregation: Range
     Field: num_focos
     Range: 0 -> 100000; 100000 -> 200000; 200000 -> 300000; 300000 -> +...; 
     Custom Label: Número de Focos
Metric
     Aggregation: Count
     Custom label: Quantidade de Anos

2. Maior/Menor Num Focos (ano)
Metric > queimadas:
     Aggregation: Max
     Field: num_focos
     Label: Número de focos (Maior)
Add (em Metric, não Buckets)
     Aggregation: Top Hit
     Field: ano
     Aggregate with/size: Concatenate/1
     Sort on: num_focos
     Order: Descending
     Custom Label: Ano

3. Número de Anos Pesquisados
Metric > queimadas
     Aggregation: Count
     Custom label: Anos Registrados
Add Metric
     Aggregation: Min
     Custom label: De
Add Metric
     Aggregation: Max
     Custom label: Até

4. Média de Focos Registrados
Metric > queimadas
     Aggregation: Average
     Field: num_focos
     Custom label: Focos em média

5. Mediana de Focos Registrados
Metric > queimadas
     Aggregation: Median
     Field: num_focos
     Custom labem: Mediana

6. Percentis/Quartis
Data Table > queimadas
Add Metric (não usa a que já tem)
     Metric: Percentile
     Field: num_focos
     Percentis: 1, 5, 25, 50, 75, 95, 99
     Custom label: focos

7. Desvio Padrão
Data Table > queimadas
Add Metric (não usa a que já tem)
     Metric: Standard Deviation 
     Field: num_focos
     Custom label: focos