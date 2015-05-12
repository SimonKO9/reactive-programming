package suggestions


import language.postfixOps
import scala.async.Async
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }

    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true
    )
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }
  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n: _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

  test("timedOut should emit events only till timeout") {
    val obs1 = Observable.interval(2 second)
    val timedOut = obs1.timedOut(3)

    var values = List[Long]()
    timedOut.toBlocking.foreach(value => values = values :+ value)

    Thread.sleep(1000 * 5)

    assert(values == List(0))
  }

  test("recovered should terminate after exception") {
    val myError = new Exception("Error")
    val obs1 = Observable.create[Long]({ observer =>
      (1 to 5).foreach(observer.onNext(_))
      Subscription()
    }) map { value =>
      if(value == 5) throw myError
      else value
    }

    val recovered = obs1.recovered.take(6)
    val values = recovered.toBlocking.toList

    val expected = List(
      Success(1), Success(2), Success(3), Success(4), Failure(myError)
    )
    assert(values === expected)
  }

  test("concatRecovered should continue on exception") {
    val myError = new Exception("Error")

    val obs1 = Observable.just[Long](1, 2, 3)
    val recovered = obs1.concatRecovered { i =>
      if(i == 2) Observable.error(myError)
      else Observable.just(i)
    }

    val values = recovered.toBlocking.toList

    val expected = List(
      Success(1), Failure(myError), Success(3)
    )

    assert(values === expected)
  }

}
