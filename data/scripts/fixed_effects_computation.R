# install.packages('tidyverse')
# install.packages('renv')
library(tidyverse)
library(arrow)
library(fixest)

DataframeFromNamedList = function(my_list, is_category_aggregated){
  # Convert named list to dataframe
  name_matrix <- do.call(rbind, strsplit(names(my_list), " _ "))
  if (is_category_aggregated){
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
  return(result_df)
}

# Оборачиваю подсчёт фиксированных эффектов в функцию
ComputeFE = function(data_directory, 
                      is_category_aggregated, 
                      value_or_quantity = 'value', 
                      plain_or_change = 'plain',
                      save_path,
                      interact_category = TRUE,
                      fixef.tol = 1e-5,
                      max_iter = 10000,
                      verbose = 1
                     ){
  
  if(data_directory == "data/preprocessed_data/trade_hs0.parquet.gzip" && is_category_aggregated){
    stop('В файле нет категории продукта!')
  }
  
  
  df = read_parquet(data_directory)
  
  # Рассчитываем приросты
  if(plain_or_change == 'change'){
    if (value_or_quantity == "value"){
      df[,'log_change'] = log(df$v) - log(df$v19)
    } else {
      df[,'log_change'] = log(df$q) - log(df$q19)
    } 
  }
  # Прописываем зависимую переменную и формируем формулу
  if(plain_or_change == 'change'){
    y = 'log_change'
  } else{
    if (value_or_quantity == "value"){
      y = 'log(v)'
    } else {
      y = 'log(q)'
    }
  }
  
  # Считаем переменные, по которым потом считаются фиксированные эффекты
  if (is_category_aggregated){
    df = df %>% mutate(export_fe = paste(t, "_", i, "_", category),
                       import_fe = paste(t, "_", j, "_", category))
    if (interact_category){
      df = df %>% mutate(bilateral_fe = paste(i, "_", j, "_", category))  
    } else{
      df = df %>% mutate(bilateral_fe = paste(i, "_", j)) 
    }
  } else {
    df = df %>% mutate(export_fe = paste(t, "_", i),
                       import_fe = paste(t, "_", j),
                       bilateral_fe = paste(i, "_", j))
   
  }

  if(interact_category){
    fml = formula(paste(y, '~1 | export_fe+import_fe+bilateral_fe'))
  } else{
    fml = formula(paste(y, '~1 | export_fe+import_fe+bilateral_fe+category'))
  }

  m = feols(fml, data = df,
   verbose = verbose,
   fixef.iter = max_iter,
   fixef.tol = fixef.tol,
  #  fixef.rm = 'both', Убирает singleton FE, но мне по смыслу стоит их оставить
   mem.clean = TRUE)
  print(m)

  fixed_effects = m %>% fixest::fixef(fixef.iter = max_iter, fixef.tol = fixef.tol)
  summary(fixed_effects) %>% print()

  if (interact_category && (data_directory != "../preprocessed_data/trade_hs0.parquet.gzip")){
    save_path = paste(save_path, '_cat_interact', sep = '')
  }

  if (!interact_category && (data_directory != "../preprocessed_data/trade_hs0.parquet.gzip")){
    save_path = paste(save_path, '_cat_not_interact', sep = '')
  }

  fixed_effects$export_fe %>%
    DataframeFromNamedList(., is_category_aggregated) %>%
    write_parquet(., paste(save_path, '_',  value_or_quantity, '_', plain_or_change, '_export_fe.parquet.gzip', sep = ''))
  fixed_effects$import_fe %>%
    DataframeFromNamedList(., is_category_aggregated) %>%
    write_parquet(., paste(save_path, '_',  value_or_quantity, '_', plain_or_change, '_import_fe.parquet.gzip', sep = ''))

}


