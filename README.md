<div align="center">

<img src="docs/images/Hero.png" alt="Witch Logo" width="120"/>

# [Witch](https://github.com/yourusername/witch) - AI Data Analyst

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18+-61DAFB?style=flat-square&logo=react&logoColor=black)](https://react.dev)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-412991?style=flat-square&logo=openai&logoColor=white)](https://openai.com)
[![License](https://img.shields.io/badge/License-Proprietary-red?style=flat-square)](LICENSE)
[![PRs](https://img.shields.io/badge/PRs-Not%20Accepted-red?style=flat-square)]()

[![code style](https://img.shields.io/badge/code%20style-black-000000?style=flat-square)](https://github.com/psf/black)
[![Vite](https://img.shields.io/badge/Vite-646CFF?style=flat-square&logo=vite&logoColor=white)](https://vitejs.dev)
[![TailwindCSS](https://img.shields.io/badge/Tailwind-06B6D4?style=flat-square&logo=tailwindcss&logoColor=white)](https://tailwindcss.com)

**Talk to your database in plain English — get SQL, insights, and ML-ready datasets** 🧙‍♀️

*This is proprietary software. Viewing for educational purposes only. See [LICENSE](LICENSE).*

</div>

---

## 🖥️ Screenshots

<div align="center">

### Terminal Interface
| Landing Page | Database Connection |
|:------------:|:-------------------:|
| <img src="docs/images/Hero.png" width="400"/> | <img src="docs/images/Connection.png" width="400"/> |

### AI Chat — Ask Questions, Get SQL
| Natural Language Query | SQL Generation | Results |
|:----------------------:|:--------------:|:-------:|
| <img src="docs/images/chat_1.png" width="280"/> | <img src="docs/images/chat_2.png" width="280"/> | <img src="docs/images/chat_3.png" width="280"/> |

### ML Feature Engineering Dashboard
| Define Grain | Define Target | Build Features | Export |
|:------------:|:-------------:|:--------------:|:------:|
| <img src="docs/images/dashboard_1.png" width="220"/> | <img src="docs/images/dashboard_2.png" width="220"/> | <img src="docs/images/dashboard_3.png" width="220"/> | <img src="docs/images/dashboard_4.png" width="220"/> |

### Data Quality Audit
<img src="docs/images/Audit.png" width="600"/>

</div>

---

## ✨ Features

<table>
<tr>
<td width="50%">

### 🔮 Natural Language to SQL
Ask questions in plain English, get production-ready SQL

### 📊 ML Feature Engineering  
Build observation-aware features with leakage prevention

### 🎯 Click-to-Select Target
Define target variables without writing code

</td>
<td width="50%">

### ⚡ Real-time Validation
SQL syntax checking and data quality analysis

### 🔒 No Data Leakage
Automatic temporal isolation for ML features

### 📦 Export Ready
Complete SQL packages with metadata

</td>
</tr>
</table>

---

## 🏗️ Architecture

```mermaid
flowchart TB
    subgraph Frontend["🖥️ Frontend (React + Vite)"]
        UI[Terminal UI]
        Wizard[ML Wizard]
        Chat[AI Chat]
    end

    subgraph API["⚡ API Layer (FastAPI)"]
        Router[API Router]
        Auth[Session Manager]
    end

    subgraph Services["🔧 Services"]
        DB[DB Service]
        Schema[Schema Service]
        Grain[Grain Service]
        Target[Target Service]
        Feature[Feature Service]
        Quality[Quality Service]
        LLM[LLM Service]
    end

    subgraph Data["💾 Data"]
        PG[(PostgreSQL)]
        MySQL[(MySQL)]
    end

    subgraph External["🌐 External"]
        OpenAI[OpenAI GPT-4]
    end

    Frontend --> API
    API --> Services
    Services --> Data
    LLM --> OpenAI
```

```mermaid
sequenceDiagram
    participant U as User
    participant F as Frontend
    participant A as API
    participant AI as OpenAI
    participant D as Database

    U->>F: "Show me top 10 customers"
    F->>A: POST /api/chat
    A->>AI: Generate SQL
    AI-->>A: SELECT * FROM...
    A->>D: Execute Query
    D-->>A: Results
    A-->>F: JSON + Visualization
    F-->>U: Table & Charts
```

---

## 📈 Who Is This For?

| Role | Use Case |
|------|----------|
| **👨‍💼 Business Analysts** | Ask questions in plain English — no SQL needed |
| **👩‍🔬 Data Scientists** | Build ML datasets with proper train/test splits |
| **👨‍💻 Data Engineers** | Validate data quality, generate reproducible SQL |
| **🏢 Teams** | Connect to any SQL database, collaborate securely |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Frontend** | React 18, Vite, TailwindCSS |
| **Backend** | Python 3.11, FastAPI, SQLAlchemy |
| **Database** | PostgreSQL, MySQL |
| **AI** | OpenAI GPT-4, LangChain |
| **UI Theme** | Bloomberg Terminal-inspired |

---

## 📁 Structure

```
witch/
├── witch_backend/          # Python FastAPI
│   ├── app/api/           # 50+ REST endpoints
│   ├── app/services/      # Business logic
│   └── requirements.txt
│
├── witch_frontend/         # React + Vite
│   ├── src/components/    # UI Components
│   └── package.json
│
└── docs/images/           # Screenshots
```

---

## 📄 License

**⚠️ Proprietary Software — All Rights Reserved**

This repository is for viewing purposes only. Commercial use, copying, modification, or distribution requires explicit written permission and a paid license.

For licensing inquiries: [your-email@example.com]

---

<div align="center">

**Built for data scientists who hate data leakage** 🧙‍♀️

</div>
