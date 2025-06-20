# ETL - Automação de Processos

## Data Project - Automação de Processos ETL

<div align='center'>

![ETL](https://github.com/user-attachments/assets/514bfda7-04a7-4e12-9a8c-7a605ab28ab6)

</div>

# 1. Problema do Negócio
<p align='justify'>Uma empresa do setor de entretenimento de mídia enfrentava desafios significativos na agilidade e precisão de suas análises de mercado. O processo de extração, transformação e carregamento (ETL) de dados do vasto repositório público do IMDb era manual e demorado. Essa dependência de processos manuais resultava em atrasos na disponibilização de informações, limitando a capacidade da equipe de analistas de responder rapidamente às dinâmicas do mercado e de gerar insights assertivos em tempo hábil. A lentidão e a propensão a erros no fluxo de dados comprometiam a eficácia das decisões estratégicas.</p>
<p align='justify'>Reconhecendo a urgência de otimizar sua inteligência de mercado, a empresa decidiu investir na automação completa do processo ETL dos dados do IMDb. O objetivo era prover à equipe de análise o acesso rápido e confiável a dados limpos e prontos para uso, garantindo que as decisões de conteúdo, marketing e estratégia fossem baseadas em informações atualizadas e precisas.</p>
<p align='justify'>Diante dessa urgência, a organização tomou a decisão estratégica de investir na automação completa do seu pipeline ETL para os dados do IMDb. O principal objetivo era capacitar a equipe de análise, proporcionando-lhes acesso rápido e confiável a dados limpos e já preparados para consumo. Essa iniciativa visava garantir que todas as decisões relacionadas a conteúdo, marketing e estratégia fossem fundamentadas em informações atualizadas e de alta precisão.</p>
<p align='justify'>Como benefícios propostos por esta solução, se esperava observar:</p>
<p align='justify'>- Agilidade na Análise: Redução drástica no tempo de preparação dos dados, permitindo que os analistas se concentrassem na geração de insights, e não na manipulação de dados.</p>
<p align='justify'>- Precisão dos Dados: Minimização de erros humanos inerentes a processos manuais, garantindo a integridade e a confiabilidade das informações.</p>
<p align='justify'>- Otimização de Recursos: Liberação da equipe de análise de tarefas repetitivas de ETL, permitindo que direcionassem seus esforços para atividades de maior valor agregado.</p>
<p align='justify'>- Tomada de Decisão Aprimorada: Suporte a decisões estratégicas mais rápidas e bem-informadas, impulsionando a competitividade no dinâmico mercado de entretenimento.</p>

# 2. Premissas do Negócio
<p align='justify'>Para o desenvolvimento desta solução, foram consideradas as seguintes premissas relacionadas aos dados do IMDb:</p>
<p align='justify'>- Fonte de Dados Pública: A solução se baseia exclusivamente nos datasets públicos disponibilizados pelo IMDb, garantindo a acessibilidade e a replicação do processo.</p>
<p align='justify'>- Volume e Estrutura dos Dados: A automação é robusta o suficiente para lidar com o grande volume e a complexidade estrutural dos dados do IMDb, que incluem informações sobre títulos (filmes, séries), elenco, equipes de produção, avaliações, gêneros, entre outros.</p>
<p align='justify'>- Frequência de Atualização: O processo automatizado é capaz de incorporar novas atualizações do IMDb periodicamente, garantindo que as análises sempre reflitam os dados mais recentes disponíveis.</p>
<p align='justify'>- Destino dos Dados: Os dados transformados são carregados em um formato e local acessível para a equipe de analistas, permitindo fácil integração com ferramentas de visualização e análise de dados (ex: SQL Database ou arquivos CSV prontos para consumo). A principal base de dados utilizada é o conjunto de arquivos .tsv (Tab Separated Values) fornecidos pelo IMDb, que incluem informações de títulos, nomes, ratings e associações.</p>

# 3. Estratégia da Solução
<p align='justify'>A estratégia da solução focou na construção de um pipeline ETL robusto e automatizado, utilizando Python como a linguagem principal para orquestrar todas as etapas do processo. A metodologia adotada incluiu as seguintes fases:</p>
<p align='justify'>Extração (Extract): A etapa de Extração é o ponto de partida do nosso pipeline, focada em adquirir os dados brutos necessários. Nela, desenvolvemos scripts Python para baixar diretamente os datasets mais recentes de repositórios externos. Esta fase envolveu:</p>
<p align='justify'>- Download Direto de Arquivos Remotos: A funcionalidade permite a obtenção de uma lista específica de arquivos de uma URL base, garantindo que os dados mais atualizados sejam sempre a fonte de informação.</p>
<p align='justify'>- Gestão Inteligente de Downloads: Antes de iniciar o download, a ferramenta verifica a existência local de cada arquivo. Isso evita downloads desnecessários, otimizando o uso de largura de banda e tempo de processamento.</p>
<p align='justify'>- Armazenamento Local: Os arquivos baixados são armazenados de forma organizada em um diretório local (data/), servindo como o repositório inicial para as próximas etapas de transformação e carga.</p>
<p align='justify'>- Registro e Tratamento Básico de Erros: O processo inclui um sistema de log para monitorar o andamento de cada download e identificar falhas (como problemas de conexão ou arquivos não encontrados), registrando o status e códigos de erro para depuração.</p>
<p align='justify'>Transformação (Transform): Preparação dos dados para análise e carregamento. Foram implementadas funções Python para ler, limpar e organizar os arquivos de dados brutos. Esta fase envolveu:</p>
<p align='justify'>- Leitura de Dados Compactados: Leitura eficiente de arquivos no formato Gzip (.gz), descompactando-os automaticamente para processamento.</p>
<p align='justify'>- Tratamento de Valores Nulos Específicos: Padronização de dados ao substituir a string \N (comum em datasets como o IMDb para representar valores nulos) por None, garantindo consistência.</p>
<p align='justify'>- Amostragem de Dados: Para otimização de desempenho e testes, apenas as primeiras 1.000 linhas de cada arquivo são processadas nesta etapa, o que é útil em ambientes de desenvolvimento ou para validação rápida.</p>
<p align='justify'>- Reescrita de Dados: Os dados transformados são salvos em um novo diretório (data/processed), no formato TSV (valores separados por tabulação), sem a extensão de compactação original, preparando-os para a próxima fase do pipeline.</p>
<p align='justify'>Carregamento (Load): etapa final do pipeline ETL, onde os dados já transformados são disponibilizados em um formato e destino prontos para consumo. Essa etapa envolveu:</p>
<p align='justify'>- Carregamento para Banco de Dados Relacional (SQLite): Os scripts Python foram configurados para coletar os arquivos .tsv processados (gerados na etapa de Transformação) e carregá-los diretamente em um banco de dados SQLite. Isso permite o uso de consultas SQL complexas para análises e extração de insights, aproveitando a capacidade de relacionamento de dados de um banco de dados relacional.</p>
<p align='justify'>- Criação Dinâmica de Tabelas: Para cada arquivo .tsv encontrado no diretório de dados processados, uma nova tabela é criada no banco de dados. Os nomes das tabelas são derivados dos nomes dos arquivos originais, com sanitização de caracteres (pontos e hífens são substituídos por underscores) para garantir compatibilidade com padrões SQL.</p>
<p align='justify'>- Atualização e Substituição: A estratégia de carregamento utilizada garante que, se uma tabela já existir no banco de dados com o mesmo nome, ela será substituída pelos dados mais recentes. Isso assegura que o banco de dados esteja sempre atualizado com a versão mais recente dos datasets processados.</p>
<p align='justify'>A escolha de Python para este projeto não apenas garantiu flexibilidade e escalabilidade, mas também permitiu a integração com a vasta gama de bibliotecas de manipulação de dados (como Pandas) e automação, tornando o processo de carregamento eficiente e robusto.</p>

# 4. Insights de Dados
<p align='justify'>Com o processo ETL automatizado, a equipe de analistas conseguiu explorar os dados do IMDb com muito mais eficiência, gerando insights valiosos para o negócio. Exemplos de análises e insights obtidos incluem:</p>
<p align='justify'>- Desempenho por Gênero: Identificação dos gêneros cinematográficos com maior número de títulos, maiores avaliações médias e maior volume de votos, revelando tendências de popularidade e preferências do público. Por exemplo, gêneros como Drama e Comédia frequentemente lideram em quantidade, mas Sci-Fi e Fantasia podem apresentar avaliações médias mais altas em títulos recentes.</p>
<p align='justify'>- Tendências Temporais: Análise da evolução do número de lançamentos de filmes/séries e suas avaliações ao longo dos anos, permitindo identificar períodos de alta produção ou mudanças no gosto do público.</p>
<p align='justify'>- Popularidade de Atores/Diretores: Classificação dos profissionais mais influentes com base no número de produções e nas avaliações médias de seus trabalhos, auxiliando em decisões de casting e colaborações.</p>
<p align='justify'>- Distribuição de Avaliações: Análise da distribuição das notas dos filmes, identificando se a maioria tende a ser bem avaliada ou se há uma polarização de opiniões em determinados títulos ou gêneros.</p>
<p align='justify'>- Correlação entre Votos e Avaliação: Investigação da relação entre o número de votos de um título e sua avaliação média, para entender se títulos com muitos votos tendem a ter avaliações mais extremas (positivas ou negativas). Esses insights fornecem à empresa uma base sólida para direcionar suas estratégias de aquisição de conteúdo, produção original, e campanhas de marketing, otimizando o engajamento da audiência e o retorno sobre o investimento.</p>
<p align='justify'>A fim de testar efetivamente o processo ETL automatizado, foram criadas duas tabelas para análise, sendo:</p>
<p align='justify'>- title_analytics: Tabela que agrega informações básicas sobre títulos (como tipo, ano e gênero) com seus dados de avaliação (nota média e número de votos), além de calcular o número de participantes únicos associados a cada título. O resultado é uma visão consolidada e pronta para análises de popularidade e engajamento.</p>
<p align='justify'>- participants_analytics: Tabela que combina os detalhes dos participantes de cada título (identificador, ordem, categoria de envolvimento) com os gêneros dos títulos em que atuaram. Isso permite análises sobre a correlação entre participantes e gêneros cinematográficos.</p>
<p align='justify'>A função analytic_views, que simboliza a criação de tabelas analíticas, é um componente estratégico do pipeline de dados. Ela transforma os dados brutos e já carregados em estruturas que são:</p>
<p align='justify'>- Otimizadas para Análise: Ao pré-computar e agregar informações, as tabelas resultantes eliminam a necessidade de consultas complexas em tempo real, tornando as análises mais ágeis e eficientes.</p>
<p align='justify'>- Consistentes e Atualizadas: A estratégia de descarte e recriação garante que os analistas e aplicações downstream sempre acessem a versão mais atual e precisa dos dados.</p>
<p align='justify'>- Fundamentais para BI e Relatórios: Essas tabelas são frequentemente a base para dashboards de Business Intelligence (BI), relatórios gerenciais e outras aplicações que consomem dados de forma intensiva, oferecendo uma visão clara e sumarizada das métricas de interesse.</p>
<p align='justify'>Essa etapa representa a materialização do esforço de extração e transformação, resultando em um conjunto de dados prontamente acessível para equipe de Inteligência de Negócios.</p>

# 5. Produto Final
<p align='justify'>O produto final desta solução é um pipeline ETL (Extract, Transform, Load) totalmente automatizado e robusto, desenvolvido em Python. Este pipeline é responsável por baixar, processar e carregar os dados do repositório público do IMDb para um destino centralizado e acessível, onde a equipe de analistas pode facilmente consultá-los. Ele garante que os dados estejam sempre limpos, atualizados e prontos para serem utilizados em qualquer ferramenta de análise ou visualização de dados.</p>
<p align='justify'>A automação abrange desde a aquisição dos dados brutos até a sua transformação em um formato padronizado e a inserção em um ambiente de fácil acesso, eliminando a necessidade de intervenção manual e permitindo que os analistas foquem exclusivamente na extração de valor destes dados.</p>

# 6. Conclusão
<p align='justify'>A automação do processo ETL dos dados do IMDb representa um avanço estratégico para a organização, em especial à equipe de Dados. Ao eliminar a morosidade e a suscetibilidade a erros dos processos manuais, a solução permite à equipe de analistas gerar insights com maior agilidade e precisão. Isso permite que a empresa tome decisões mais rápidas e assertivas em um mercado altamente competitivo, otimizando suas estratégias de conteúdo e ampliando sua inteligência de mercado.</p>
<p align='justify'>O pipeline em Python não é apenas uma ferramenta, mas um catalisador para uma cultura de análise de dados mais eficiente e orientada a resultados, fortalecendo a posição da organização no mercado.</p>

# 7. Próximos Passos
<p align='justify'>Como próximos passos, aprimoramentos e expansões para o pipeline automatizado serão considerados:</p>
<p align='justify'>Implementação de um Data Warehouse ou Data Lake: Avaliar a migração dos dados processados para um ambiente de Data Warehouse (e.g., Snowflake, Google BigQuery) ou um Data Lake (e.g., Amazon S3, Google Cloud Storage). Isso otimizaria o armazenamento de grandes volumes de dados históricos e facilitaria consultas complexas e a integração com outras fontes de dados da empresa.</p>
<p align='justify'>Desenvolvimento de Dashboards Interativos: Conectar os dados processados e limpos a ferramentas de Business Intelligence como Power BI ou Tableau. Isso permitirá a criação de dashboards interativos e dinâmicos, que visualizam os insights gerados e facilitam o consumo das informações pelas áreas de negócio, maximizando o valor da automação ETL.</p>
