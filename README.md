# Репозиторий для ВКР

### Структура репозитория:

- `data`:
    - `data/raw_data` содержит ссылки исходные данные (сами данные на слишком объёмные для гитхаба).
    - `data/scripts` --- скрипты, с помощью которых я объединяю данные в финальный датасет (`data/processed_data`).
- `analysis`:
    - `hs0_estimation.ipynb` --- по агрегированным данным на графовых (т.е. с ключом `i+j+t`) наблюдениях  считаю эффект на панельных данных (+ на пространственных данных по годам)    .
    - `hs0_fe_estimation.ipynb` --- по агрегированным данным на фиксированных эффектах экспортёра/импортёра (`i+t`/`j+t`) считаю эффект на панельных данных.
    - `hs2_estimation.ipynb` --- по сырым данным на графовых наблюдений (`i+j+k+t`)