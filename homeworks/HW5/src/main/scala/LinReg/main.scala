package LinReg

import breeze.stats._
import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._


object main {
  class LinearRegression {

    // веса модели
    var w: DenseVector[Double] = DenseVector.fill(1)(0.1)

    def fit(X: DenseMatrix[Double],
            y: DenseVector[Double],
            n_steps: Int = 100,
            learning_rate: Double = 0.001): Unit = {
      w = DenseVector.fill(X.cols)(1)

      for (i <- 0 until n_steps) {
        for (i <- 0 until X.rows) {
          val grad = X(i, ::) * (X(i, ::) * w - y(i))
          w = w - learning_rate * grad.t
        }
        val MAE = mean(abs(y - X * w))
        println(f"Итерация: $i, MAE - $MAE")
      }
    }

    def predict(X: DenseMatrix[Double]): DenseVector[Double] = {
      val ones = DenseMatrix.fill[Double](X.rows, 1)(1)
      val X_ = DenseMatrix.horzcat(ones, X)
      return X_ * w
    }
  }


  def main(args: Array[String]): Unit = {
    val N = 100000
    val M = 3

    val norm_dist = Gaussian(0, 1)
    val X = DenseMatrix.rand(N, M, norm_dist)
    val X_ = X.copy

    val x1 = X(::,0):*= 1.5
    val x2 = X(::,1):*= 0.3
    val x3 = X(::,2):*= -0.7
    val y = x1 + x2 + x3

    val model = new LinearRegression()
    model.fit(X_, y, 100, 0.001)
    println(model.w)
  }
}