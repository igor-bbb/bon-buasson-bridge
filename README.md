# VECTRA — Business Analysis System

---

## 📌 Описание

VECTRA — это система управленческой аналитики, которая показывает:

- где теряются деньги  
- где есть потенциал  
- куда нужно идти  

Главный принцип:

Бизнес = рынок (эталон)  
Все объекты сравниваются с бизнесом  

---

## 🎯 Главный KPI

Недозаработано (в деньгах)

Дополнительно:
- % разрыва (как сигнал)

---

## 🧠 Архитектура

Система построена как STATE MACHINE

---

### STATE

level  
object  
period  
filter  
stack  
last_list  

---

Любое действие пользователя = изменение STATE

---

### FILTER

Всегда полный:

- manager_top  
- manager  
- network  
- category  
- tmc_group  
- sku  
- period  

---

### Навигация

Вниз:
- push в stack  
- обновление filter  

Назад:
- stack.pop()  
- без API  
- без пересчета  

---

### Выбор

Источник истины: last_list

---

### Команда "все"

- тот же filter  
- без limit  
- полный список  

---

### UI правило

UI ничего не считает  
UI только отображает state  

---

## 📊 Логика анализа

GAP:
- деньги — основной KPI  
- % — дополнительный сигнал  

Бизнес:
- эталон  
- не бюджет  

YoY:
- везде  
- в скобках  
- к прошлому году  

---

## 🧾 Бизнес-экран

- Оборот  
- Маржа  
- Наценка  
- Финрез до  
- Финрез итог (только здесь)  
- Ретробонус  
- Логистика  
- Персонал  
- Прочее  

---


## 🔌 Preferred API entrypoints

For ChatGPT / Actions use these screen endpoints first:

- `/business_summary` — CEO screen with KPI block and drain to manager tops
- `/manager_top_summary` — top manager screen with drain to managers
- `/manager_summary` — manager screen with drain to networks
- `/network_summary` — network screen with drain to SKU
- `/sku_summary` — final SKU screen
- `/vectra/query` — stateful natural-language entrypoint with drilldown, reasons, all, and back

Raw comparison endpoints remain available for debugging and low-level checks.

---

## 📁 Структура проекта

app/
├── domain/
├── presentation/
├── orchestration/
├── query/

---

## 📄 Документация

instruction_v2.md

---

# ⚙️ Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt