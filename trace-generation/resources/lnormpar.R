lnormpar <- function(x1, xn, xbar, n, start=c(0,1)) {
  # negative log likelihood
  nll <- function(theta) {
    mu <- theta[1]
    sigma <- theta[2]
    z1 <- (log(x1)-mu)/sigma
    z2 <- (log(xn)-mu)/sigma
    # mean and variance of (x_1,x_n)-truncated lognormal
    mu1.trunc <- exp(mu + sigma^2/2)*
      (pnorm(z2 - sigma) - pnorm(z1 - sigma))/
      (pnorm(z2) - pnorm(z1))
    mu2.trunc <- exp(2*mu + 2*sigma^2)*
      (pnorm(z2 - 2*sigma) - pnorm(z1 - 2*sigma))/
      (pnorm(z2) - pnorm(z1))
    var.trunc <- mu2.trunc - mu1.trunc^2
    # joint density of x1, xn, xbar
    ll <- 
      sum(dlnorm(c(x1,xn), mu, sigma, log=TRUE)) +
      (n-2)*log(plnorm(xn, mu, sigma) - plnorm(x1, mu,sigma)) +
      dnorm(xbar, (x1 + xn + (n-2)*mu1.trunc)/n, sqrt(var.trunc/(n-2)), log=TRUE)
    -ll
  }
  # maximise the log likelihood
  opt <- optim(start, nll, hessian=TRUE)
  # extract parameter estimates
  res <- cbind(opt$par, sqrt(diag(solve(opt$hessian))))
  rownames(res) <- c("mu","sigma")
  colnames(res) <- c("Estimate","Std. Error")
  res
}
