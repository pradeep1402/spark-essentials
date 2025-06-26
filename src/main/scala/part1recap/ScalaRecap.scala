package part1recap

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  private val aBoolean: Boolean = false
  println(s"It's a boolean: $aBoolean")

  // expressions
  private val anIfExpression = if (2 > 3) "bigger" else "smaller"
  println(s"An if expression: $anIfExpression")

  // instructions vs expressions
  val theUnit: Unit = println(
    "Hello, Scala"
  ) // Unit = "no meaningful value" = void in other languages

  // functions
  private def myFunction(x: Int) = 42
  println(s"Calling the function: ${myFunction(12)}")

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  private val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)
  println(s"Incremented value for 42: $incremented")

  // map, flatMap, filter
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  private val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  println("pattern matching: " + ordinal)

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _: Throwable            => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  private val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex)            => println(s"I have failed: $ex")
  }

  // Partial functions
  private val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  println(aPartialFunction(2))

  // Implicits

  // auto-injection by the compiler
  private def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt: Int = 67
  private val implicitCall = methodWithImplicitArgument

  println(implicitCall)

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet(): Unit = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String): Person = Person(name)
  "Bob".greet() // fromStringToPerson("Bob").greet

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark(): Unit = println("Bark!")
  }
  "Lassie".bark()

  /*
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
   */

}
