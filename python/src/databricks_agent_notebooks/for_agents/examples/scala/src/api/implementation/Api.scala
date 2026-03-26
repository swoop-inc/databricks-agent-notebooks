package api.implementation

object Api extends api.stable.Api {
  def compute(value: Double): Double = 2 * value
}
