# install.packages('tidyverse')
# install.packages('renv')
library(tidyverse)
library(arrow)
library(fixest)
setwd("C:/Users/unstr/Documents/7 семестр работы/Диплом/thesis-repo/thesis")
getwd()

# Оборачиваю подсчёт фиксированных эффектов в функцию
compute_fe = function(dataDirectory, 
                      isCategoryAggregated, 
                      valueOrQuantity = 'value', 
                      plainOrChange = 'plain'){
  
  if(dataDirectory == "data/preprocessed_data/trade_hs0.parquet.gzip" & isCategoryAggregated){
    stop('В файле нет категории продукта!')
  }
  
  df = read_parquet(dataDirectory)
  
  # Рассчитываем приросты
  if(plainOrChange == 'change'){
    if (valueOrQuantity == "value"){
      df[,'log_change'] = log(df$v) - log(df$v19)
    } else {
      df[,'log_change'] = log(df$q) - log(df$q19)
    } 
  }
  # Прописываем зависимую переменную и формируем формулу
  if(plainOrChange == 'change'){
    y = 'log_change'
  } else{
    if (valueOrQuantity == "value"){
      y = 'log(v)'
    } else {
      y = 'log(q)'
    }
  }
  fml = formula(paste(y, '~1| export_fe+import_fe+bilateral_fe'))
  # Считаем переменные, по которым потом считаются фиксированные эффекты
  if (isCategoryAggregated){
    df = df %>% mutate(export_fe = paste(t, "_", i, "_", category),
                       import_fe = paste(t, "_", j, "_", category),
                       bilateral_fe = paste(i, "_", j, "_", category))
  } else {
    df = df %>% mutate(export_fe = paste(t, "_", i),
                       import_fe = paste(t, "_", j),
                       bilateral_fe = paste(i, "_", j))
   
  }
  print('Считаем фиксированные эффекты')
  m = feols(fml, data = df)
  list(model = m, fe = m %>% fixef())
}

dataframe_from_named_list = function(my_list, isCategoryAggregated){
  # Convert named list to dataframe
  name_matrix <- do.call(rbind, strsplit(names(my_list), " _ "))
  if (isCategoryAggregated){
    result_df <- data.frame(
      year = name_matrix[, 1],
      country = name_matrix[, 2],
      category = name_matrix[, 3],
      value = unlist(my_list),
      stringsAsFactors = FALSE
    )
  } else{
    result_df <- data.frame(
      year = name_matrix[, 1],
      country = name_matrix[, 2],
      value = unlist(my_list),
      stringsAsFactors = FALSE
    )
  }
  result_df$year <- as.integer(result_df$year)
  result_df
}


res = compute_fe("data/preprocessed_data/trade_hs0.parquet.gzip", 
                 isCategoryAggregated = F, 
                 valueOrQuantity = "value",
                 plainOrChange = 'plain') 
res$model
res$fe$export_fe %>% dataframe_from_named_list(., F) %>% head()

