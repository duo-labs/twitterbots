data_journalist <- read.csv("journalist_followers.csv")

rle_output <- rle(data_journalist$Created.Date)

# computing threshold by seeing what is more than three standard deviations above the mean
threshold <- ceiling(mean(rle_output$lengths) + 3 * sd(rle_output$lengths))
followers_in_a_row <- rle_output$lengths[rle_output$lengths > threshold]
length_index <- which(rle_output$lengths > threshold)

# computing the index for the group of followers
follower_start_index <- numeric(length(length_index))
for (i in 1:length(length_index)){
  follower_start_index[i] <- sum(rle_output$lengths[1:(length_index[i] - 1)]) + 1
}

follower_end_index <- follower_start_index + followers_in_a_row - 1

potential_fake_followers <- data.frame(followers_start_index = follower_start_index,
                                       followers_end_index = follower_end_index,
                                       followers_in_a_row = followers_in_a_row)

# some groups of fake followers may be seperated by 1 or 2 legitimate followers
# we want to combine those groups into bigger chunks
gaps <- numeric(nrow(potential_fake_followers) - 1)
for(i in 1:(nrow(potential_fake_followers) - 1)) {
  gaps[i] <- potential_fake_followers[(i + 1), c("followers_start_index")] - potential_fake_followers[i, c("followers_end_index")]
}

# getting start and end indexes of potential fake follower groups to combine
combine_matrix <-  matrix(, nrow=ceiling(nrow(potential_fake_followers)/2), ncol = 2)
if (length(which(gaps < 5)) > 0){
  i <- 1
  matrix_row <- 1
  while(i <= length(gaps)){
    if(gaps[i] < threshold){
      first_index <- i
      i <- i +1
      while(gaps[i] < 5){
        i <- i + 1
      }
      last_index <- i
      combine_matrix[matrix_row, 1] <- first_index
      combine_matrix[matrix_row, 2] <- last_index
      matrix_row <- matrix_row + 1
    }
    i <- i + 1
  }
}

colnames(combine_matrix) <- c("start_index", "end_index")
combine_matrix <- combine_matrix[complete.cases(combine_matrix), ]
combine_df <- as.data.frame(combine_matrix)

# getting final dataframe with follower indexes
final_fake_followers_rows <- matrix(, nrow=nrow(potential_fake_followers), ncol = 2) 
colnames(final_fake_followers_rows) <- c("start_index", "end_index")
i <- 1
while (i <= nrow(potential_fake_followers)){
  if (i %in% combine_df[, 1]) {
    final_fake_followers_rows[i, 1] <- potential_fake_followers[i, 1]
    final_fake_followers_rows[i, 2] <- potential_fake_followers[combine_df[combine_df$start_index == i, 2], 2]
    i <- combine_df[combine_df$start_index == i, 2]
  } else {
    final_fake_followers_rows[i, 1] <- potential_fake_followers[i, 1] 
    final_fake_followers_rows[i, 2] <- potential_fake_followers[i, 2]
  }
  i <- i + 1
}

final_fake_followers_df <- as.data.frame(final_fake_followers_rows)
final_fake_followers_df <- final_fake_followers_df[complete.cases(final_fake_followers_df), ]
