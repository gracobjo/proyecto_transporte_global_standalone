# 📊 Presentación del Proyecto SIMLOG

## Sistema Integrado de Monitorización y Simulación Logística

---

## 🎯 Objetivo

Plataforma de simulación y monitorización de red de transporte en España usando stack Apache (Kafka, Spark, GraphFrames, Cassandra, Hive).

---

## 🏗️ Arquitectura

```
Ingesta (KDD) → Procesamiento (Grafos) → Persistencia (Cass+Hive)
                                           ↓
                                    Dashboard (Streamlit)
```

---

## 🧠 Tecnologías

- Apache Kafka - Mensajería
- Apache Spark - Procesamiento
- GraphFrames - Grafos
- Apache Cassandra - BD NoSQL
- Apache Hive - Data Warehouse
- Streamlit - Dashboard
- Airflow - Orquestación

---

## 📚 Obsidian Vault

El proyecto incluye un vault de Obsidian con:

| Sección | Contenido |
|---------|-----------|
| `01_Conceptos/` | Glosario técnico |
| `02_Proyectos/` | Proyecto SIMLOG |
| `05_Snippets_Code/` | Código |
| `06_Modelos_IA/` | Modelos ML |
| `07_Documentacion/` | Manuales |

### Cómo Acceder

1. **Obsidian** → Open Vault → Seleccionar carpeta
2. **Principal**: `Bienvenido.md`
3. **Buscar**: `Ctrl+O`

---

## 🚀 Inicio Rápido

```bash
docker-compose up -d
```

---

*Presentado: {{date}}*
