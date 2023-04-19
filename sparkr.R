#load libaries
library (sparklyr)
library (dplyr)


sc <- spark_connect(master="local")

iris <- iris %>% mutate (id = seq(1:nrow(iris)))

#load sample dataset
df <- sdf_copy_to(sc, iris, overwrite = T)

mtcars <- mtcars %>% mutate (id = seq(1:nrow(mtcars)))

cars <- sdf_copy_to (sc, mtcars, overwrite = T)

summarize_all(cars, mean)

#check sthe objects loaded in spark
src_tbls(sc)


stringdist_join(spark_dataframe (iris), spark_dataframe (mtcars), 
                by = "id",
                mode = "right",
                ignore_case = TRUE, 
                method = "jw", 
                max_dist = 99, 
                distance_col = "dist") %>%
  group_by(id.x) %>%
  slice_min(order_by = dist, n = 1) 

# data_e <- copy_to (sc, data_e, "data_e", overwrite = TRUE)
# online_data_b <- copy_to (sc, online_data_b, "online_data_b")




summarize_all (data_e, mean)
summary (online_data_b)
summary (data_e)


cars <- copy_to (sc, mtcars)

database <- copy_to (sc, data_e)

portal <- copy_to (sc, online_data_b)

df2 <- spark_dataframe(cars)






