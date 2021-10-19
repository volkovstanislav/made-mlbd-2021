package test

/**
 * Scala class with LinearRegression algorithm implementation
 *
 * @author Stanislav Volkov
 */

import java.io._
import breeze.linalg._
import scala.util.Random
import breeze.stats.distributions.Gaussian


class LinearRegression(var learning_rate : Double = 0.01,
                                var n_steps: Int = 10000) {

  var weights: DenseVector[Double] = DenseVector.ones[Double](0)

  def calc_grad(X: DenseMatrix[Double],
                y: DenseVector[Double]): DenseVector[Double] ={
    return 2 * 1.0 / X.rows * X.t * (X * weights - y)
  }

  def fit(X: DenseMatrix[Double], y: DenseVector[Double]): DenseVector[Double] = {
    weights = DenseVector.ones[Double](X.cols)

    for (i <- 1 to n_steps) {
      weights += - learning_rate * calc_grad(X, y)
    }
    return weights
  }

  def predict(X: DenseMatrix[Double]): DenseVector[Double] = {
    return X * weights;
  }

  def loss(X: DenseMatrix[Double], y: DenseVector[Double]): Double = {
    val y_pred = predict(X)
    val subtraction = y.toArray zip y_pred.toArray map ( z => scala.math.pow(z._1 - z._2, 2))
    val error = 1.0 * subtraction.sum / y.length
    return error
  }
}

object Main {
  def main(args: Array[String]): Unit = {

    // Искусственный игровой пример для проверки качества работы модели с весами
    val X = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0), (10.0, 2.0, 1.0))
    val target = DenseMatrix((10.0, 28.0, 46.0, 35.0))
    val real_weights = DenseVector(3.0, 2.0, 1.0)
    val y = target.toDenseVector

    var model = new LinearRegression(learning_rate=0.01, n_steps=100000)
    model.fit(X, y)

    val mod_pred = model.predict(X)
    val mod_w = model.weights
    val tr_loss = model.loss(X, y)
    println("-------  Simple example fo testing model ---------- START ")
    println(s"Predict values: {$mod_pred}")
    println(s"Actual values: {$y}")
    println(s"Predict weights: {$mod_w}")
    println(s"Actual weights: {$real_weights}")
    println(s"Train Loss: {$tr_loss}")
    println("--------------------------------------------------- END")

    // Теперь загрузим боевые примеры с валидацией модели
    val X_data = csvread(new File("/Users/stanislav/IdeaProjects/MADE_second/x_data.csv"),',')
    val y_data = csvread(new File("/Users/stanislav/IdeaProjects/MADE_second/y_data.csv"),',')

    println("Training for real dataset ------------------------ START")
    println("X shape: ", X_data.rows, X_data.cols)

    // Train-Test split and predict result
    val trainTestSplitRatio = 0.8 // 80% for training
    val x_shape = X_data.rows / 10

    val index_train = (1 to (trainTestSplitRatio * x_shape).toInt).toSeq
    val index_test = ((trainTestSplitRatio * x_shape).toInt to (x_shape - 1).toInt).toSeq

    val X_train = X_data(index_train, ::)
    val X_test = X_data(index_test, ::)
    val y_train = y_data(index_train, ::).toDenseVector
    val y_test = y_data(index_test, ::).toDenseVector

    println("X train: ", X_train.rows, X_train.cols)
    println("X test: ", X_test.rows, X_test.cols)
    println("y train: ", y_train.length)
    println("y test: ", y_test.length)

    val lin_reg = new LinearRegression(learning_rate=0.01, n_steps=10000)
    lin_reg.fit(X_train, y_train)

    val y_train_predict = lin_reg.predict(X_train)
    val y_test_predict = lin_reg.predict(X_test)

    val train_loss = lin_reg.loss(X_train, y_train)
    val test_loss = lin_reg.loss(X_test, y_test)

    println(s"Train Loss: {$train_loss}")
    println(s"Test Loss: {$train_loss}")

    // Теперь сохраним результаты предсказаний
    csvwrite(new File("/Users/stanislav/IdeaProjects/MADE_second/train_predict.txt"),
      y_train_predict.toDenseMatrix, separator=',')
    csvwrite(new File("/Users/stanislav/IdeaProjects/MADE_second/test_predict.txt"),
      y_test_predict.toDenseMatrix, separator=',')

    println("File with predcit save --- ")
    println("Training for real dataset ------------------------ END")
  }
}

