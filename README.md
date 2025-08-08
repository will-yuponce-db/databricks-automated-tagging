# 🏷️ Databricks Autotagger

Automatic table and column tagging for Databricks Unity Catalog using NLP-based entity detection.

---

## 🚀 Overview

**Databricks Autotagger** is a PySpark-based tool for Databricks Unity Catalog environments.  
It uses spaCy’s NER 📚 to automatically detect entity types within your data and sets useful column and table tags to help with data discovery, governance, and compliance.

---

## ✨ Features

- 🤖 **Entity Detection with spaCy:**  
  Scans sample data to identify column content such as `EMAIL`, `GPE` (geopolitical), `PERSON`, and more.

- 🏷️ **Automatic Tagging:**  
  Tags columns with detected entity types and applies table-level tags according to configurable rules.

- 🔄 **Tag Propagation & Merge:**  
  Tags from source tables are merged and propagated during table joins.

- ⚙️ **Configurable Rules:**  
  Use custom rules and a tag hierarchy for full control.

- 🗂️ **Catalog-Wide Processing:**  
  Bulk-tag all schemas and tables in a Unity Catalog catalog.

---

## 💻 Requirements

- A Databricks workspace with **Unity Catalog** enabled
- PySpark (`pyspark`)
- [spaCy](https://spacy.io/) + the English model `en_core_web_sm`

---

## 🛠️ Setup

**Download and Install the package in your notebook:**

```python
pip install <PATH_TO_MODULE>
```

---

## ⚡ Usage

### 1️⃣ Define Tagging Configuration

```python
config = {
    "table_rules": [
        {
            "tag_key": "contains_pii",
            "tag_value": "true",
            "key_words": ["PERSON", "EMAIL"],
            "operation": "or"
        },
        # Add more rules as needed!
    ],
    "tag_hierarchy": [
        {
            "sensitivity": 
            {
                "public": 1,
                "internal": 2,
                "confidential": 3
            }
        },
        # Add more tag hierarchies as needed!
    ]
}
```

### 2️⃣ Create the Autotagger

```python
from autotagger import AutoTagger

autotagger = AutoTagger(config)
```

### 3️⃣ Tag Your Catalog

```python
autotagger.apply_tagging_to_catalog("my_catalog")
```

### 4️⃣ Propagate Tags During Joins

```python
autotagger.join_and_write_with_metadata(
    table_name1="my_catalog.schema1.tableA",
    table_name2="my_catalog.schema2.tableB",
    on="id",
    how="inner",
    save_as="my_catalog.schema3.joined_table"
)
```


---

## 🔍 How It Works

- **Column Tagging 🧩:** Samples data per column, detects entities with spaCy, and tags columns if an entity is found.
- **Table Tagging 🚩:** Applies rules using detected column tags (supports AND/OR logic).
- **Join & Propagate ⬆️:** Inherits and merges tags on joined tables/columns.
- **Bulk Tagging 🎛️:** Discovers all tables in a catalog and applies tagging automatically.

---

## ⚠️ Notes & Limitations

- **Performance:** Only samples a small number of rows (default = 10) for NER. Adjust as needed for your data!
- **Entity Support:** Only those recognized by spaCy’s `en_core_web_sm` unless you extend `get_entity_label`.
- **Unity Catalog:** Requires Unity Catalog features to be enabled.

---

## 🛠️ Customization

- **Change or add tag rules** in your config.
- **Extend entity detection** by editing `get_entity_label`.

---

## 📜 License

MIT

---

**🙌 Contributions welcome!**  
If you have improvements or suggestions, please open a PR or issue!




