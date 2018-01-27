import org.scalatest.FunSuite

class ratingTest extends FunSuite {
  test("reading correct File") {
    rating.readCsvFile("src/test/resources/data/rating.csv")
  }

  test("Main") {
    rating
  }
}
