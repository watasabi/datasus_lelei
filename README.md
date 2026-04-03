<a name="readme-top"></a>

<div align="center">
  <h1 align="center">datasus_lelei</h1>
  <p align="center">
    Internações renais (SIH-RD, CIDs N17–N19) — escopo Sul e Sudeste: extração, limpeza, EDA e análise de drift temporal (EWT, changepoints, Bayes).
    <br />
    <br />
    <img src="https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/Status-Development-yellow?style=for-the-badge" alt="Status">
  </p>
</div>


<details>
  <summary>Tabela de Conteúdos</summary>
  <ol>
    <li><a href="#sobre-o-projeto">Sobre o Projeto</a>
      <ul>
        <li><a href="#documentacao">Documentação</a></li>
        <li><a href="#principais-stakeholders">Principais Stakeholders</a></li>
      </ul>
    </li>
    <li><a href="#organizacao-e-estrutura">Organização e Estrutura</a></li>
    <li><a href="#documentacao-detalhada">Documentação detalhada</a></li>
    <li><a href="#pipeline-de-dados-e-analises">Pipeline de dados e análises</a></li>
    <li><a href="#configuracao-de-ambiente">Configuração de Ambiente</a></li>
    <li><a href="#convencao-de-commits">Convenção de Commits</a></li>
    <li><a href="#autor">Autor</a></li>
    <li><a href="#apendice-uv-sub-projects">Apêndice: UV Sub-projects</a></li>
  </ol>
</details>

---

## Sobre o Projeto

Estudo reprodutível sobre **internações hospitalares por doença renal** no SUS, usando o **SIH (RD)** com extração via **PySUS**. O recorte geográfico atual restringe-se às UFs do **Sul e Sudeste** (`PR`, `RS`, `SC`, `SP`, `MG`, `RJ`, `ES`). Inclui análise exploratória, agregados temporais e um módulo de **séries temporais** com foco em **drift** entre períodos pré-pandemia, pandemia (2020–2022) e pós-pandemia, usando decomposição **EWT**, **PELT** (`ruptures`), testes não paramétricos e modelo hierárquico simples em **PyMC** (ver documentação em `docs/doc.md`).

### Documentação

| Recurso | Link |
|---------|------|
| **Doc técnica + guia de leitura dos gráficos (ex.: estudantes de medicina)** | [docs/doc.md](docs/doc.md) |

### Principais Stakeholders

* **Vanderlei Carlos Pisaia**

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## 📂 Organização e Estrutura

Este projeto segue uma estrutura padronizada para garantir reprodutibilidade.

> **Nota sobre Convenção de Nomes:**
> Arquivos numerados (ex: `01_load_data.py`) indicam **ordem de execução** em pipelines ou análises.
> Código reutilizável (funções/classes) deve residir em `src/` ou `utils/` e ser importado.

```text
.
├── config/                 # Configurações e variáveis de ambiente
│   ├── .env                # Variáveis de ambiente (NÃO commitar!)
│   └── .env.example        # Template com as variáveis necessárias
│
├── data/                   # Dados do projeto (Geralmente ignorados pelo Git)
│   ├── external/           # Dados de fontes terceiras
│   ├── interim/            # Dados transformados intermediários
│   ├── processed/          # Dados finais prontos para modelagem
│   └── raw/                # Dados originais imutáveis
│
├── docs/                   # Documentação técnica (ex.: doc.md)
├── notebooks/
│   ├── eda/                # Análise exploratória
│   ├── processing/         # Scripts 00–02 (dados → interim)
│   ├── modeling/           # Ex.: drift temporal (PyMC, EWT, ruptures)
│   ├── get_data/           # (template) extração adicional
│   ├── training/           # (template)
│   └── qa/                 # (template)
│
├── queries/                # Queries SQL (.txt/.sql) para Databricks
│   └── get_data/           # Queries usadas por notebooks/get_data/
│
├── models/                 # Artefatos de modelos (ignorados pelo Git)
│
├── reports/
│   └── figures/            # eda/, mensal/, timeseries_drift/, ...
│
├── src/                    # Código Fonte Reutilizável (Library do projeto)
│   └── __init__.py         # Funções de engenharia de features
│
├── .cursorrules            # Regras para o Cursor AI
├── AGENT.md                # Guidelines para agentes AI
├── .gitignore              # Arquivos a serem ignorados pelo git
├── LICENSE                 # Licença do projeto
├── pyproject.toml          # Dependências e config (UV workspace)
└── README.md               # Documentação principal
```

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## Documentação detalhada

A narrativa completa da **aquisição de dados**, **linhagem**, **EWT**, **detecção de mudanças**, **inferência bayesiana** e **limitações** está em **[docs/doc.md](docs/doc.md)**, com diagramas **Mermaid** (fluxos e modelo). A secção **“Como interpretar os gráficos”** explica cada figura de EDA e de drift em **linguagem clínica**, para quem não é estatístico.

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## Pipeline de dados e análises

Na raiz do projeto, com dependências instaladas (`uv sync`):

```bash
# 1) Lotes SIH + consolidação renais.parquet (não apaga batches existentes)
uv run python notebooks/processing/00_get_data.py

# 2) Limpeza → renais_cleaned.parquet + .csv + .xlsx (em raw/ e processed/)
uv run python notebooks/processing/01_cleaning_cols.py

# 3) Agregados em data/interim/
uv run python notebooks/processing/02_aggregate_data.py

# 4) EDA (figuras em reports/figures/eda/)
uv run python notebooks/eda/00_first_eda.py
uv run python notebooks/eda/01_second_eda.py
uv run python notebooks/eda/02_second_eda_mensal.py

# 5) Drift temporal: EWT + PELT + Bayes (figuras em reports/figures/timeseries_drift/)
MPLBACKEND=Agg uv run python notebooks/modeling/03_timeseries_drift.py
```

**Dependências de modelagem:** `pyewt`, `pymc`, `ruptures`, `arviz` (pin `arviz<1` por compatibilidade com PyMC 5 — ver `pyproject.toml`).

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## ⚙️ Configuração de Ambiente

As variáveis de ambiente do projeto ficam em `config/.env`. Para configurar:

```bash
cp config/.env.example config/.env
```

Edite o arquivo `config/.env` com as credenciais necessárias:

| Variável | Descrição |
|----------|-----------|
| `DATABRICKS_TOKEN` | Token de acesso ao Databricks (DAPI) |
| `DATABRICKS_HOSTNAME` | Host do workspace Databricks |
| `DATABRICKS_HTTP_PATH` | HTTP Databricks Warehouse |

> **IMPORTANTE:** O arquivo `config/.env` está no `.gitignore` e **nunca** deve ser commitado. Use `config/.env.example` como referência.

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## 📝 Convenção de Commits

Este projeto segue o padrão **Conventional Commits**. Todas as mensagens de commit devem seguir o formato:

```
<tipo>(<escopo opcional>): <descrição>
```

### Tipos permitidos

| Tipo | Descrição |
|------|-----------|
| `feat` | Nova funcionalidade |
| `fix` | Correção de bug |
| `docs` | Alterações na documentação |
| `style` | Formatação (sem alteração de lógica) |
| `refactor` | Refatoração de código |
| `perf` | Melhoria de performance |
| `test` | Adição ou correção de testes |
| `chore` | Tarefas de manutenção |
| `infra` | Mudanças de infraestrutura |
| `imp` | Melhorias gerais |
| `breaking` | Mudança com quebra de compatibilidade |


### Exemplos

```bash
git commit -m "feat: adiciona modelo de classificação"
git commit -m "fix(pipeline): corrige leitura de dados raw"
git commit -m "docs: atualiza README com instruções de deploy"
git commit -m "refactor(src): simplifica feature engineering"
```

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## 👤 Autor

| Nome | Email |
|------|-------|
| **Rodrigo Watanabe Pisaia** | rodrigo.watanabe0107@gmail.com |

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>

## 📦 Apêndice: UV Sub-projects

Este projeto usa **UV workspaces**, o que permite criar sub-projetos com dependências isoladas dentro do mesmo repositório. Isso é útil quando você precisa, por exemplo, carregar um modelo legado que depende de versões específicas de bibliotecas que conflitam com o projeto principal.

### Quando usar sub-projects?

- Modelo antigo que requer versões específicas (ex: `scikit-learn==0.24`, `xgboost==1.5`)
- Serviço auxiliar com stack diferente
- Experimentação isolada sem afetar o ambiente principal

### Como criar um sub-project

```bash
# Dentro da raiz do projeto, crie o sub-project
uv init models/modelo_legado_v1

# Entre no sub-project e adicione as dependências específicas
cd models/modelo_legado_v1
uv add scikit-learn==0.24.2 xgboost==1.5.0
```

A estrutura resultante fica assim:

```text
.
├── pyproject.toml                  # Projeto principal (workspace root)
├── models/
│   └── modelo_legado_v1/           # Sub-project com deps isoladas
│       ├── pyproject.toml          # Dependências do modelo legado
│       └── src/
│           └── ...
├── src/                            # Código do projeto principal
└── ...
```

### Executando código dentro de um sub-project

```bash
# Rodar um script com as dependências do sub-project
uv run --package modelo_legado_v1 python predict.py

# Ou entre no diretório do sub-project
cd models/modelo_legado_v1
uv run python predict.py
```

### Referência do workspace no pyproject.toml

O UV detecta automaticamente sub-projetos. Para configuração explícita, adicione no `pyproject.toml` raiz:

```toml
[tool.uv.workspace]
members = ["models/*"]
```

> **Dica:** Cada sub-project tem seu próprio `pyproject.toml` e `.venv`, garantindo isolamento total de dependências.

<p align="right">(<a href="#readme-top">voltar ao topo</a>)</p>
