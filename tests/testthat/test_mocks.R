# context("just mucking about")
# 
# bla <- function(x) {
#   UseMethod("bla")
# }
# 
# bla.numeric <- function(x) {
#   message("Hai, I gots a numeric value!")
#   bla.character(as.character(x))
# }
# 
# bla.character <- function(x) {
#   message("How am I to act this character?")
# }
# 
# with_mock(
#   bla.character = function(x) { message("oops, I am but a lowly mock...") },
#   {
#     xx <- bla(2)
#     expect_null(xx)
#   }
# )
