# ============================================================
# Healthcare Analytics – R Shiny Dashboard
# Package Installer
# ============================================================
# Run this ONCE from the project root before launching the app:
#
#   source("shiny_app/install.R")
#
# Then launch with:
#   shiny::runApp("shiny_app")
# ============================================================

packages <- c(
  "shiny",      # Core Shiny framework
  "bslib",      # Bootstrap 5 UI components (page_navbar, value_box, cards)
  "plotly",     # Interactive charts
  "DT",         # Interactive DataTables
  "dplyr",      # Data wrangling
  "readr",      # Fast CSV reading
  "jsonlite",   # JSON parsing for audit log
  "scales",     # Number formatting helpers
  "lubridate"   # Date utilities
)

cat("=======================================================\n")
cat("  Healthcare Analytics – Installing R packages\n")
cat("=======================================================\n\n")

for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("  [ ] Installing %-15s ...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org", quiet = TRUE)
    cat(sprintf("  [x] %-15s installed\n", pkg))
  } else {
    cat(sprintf("  [x] %-15s already installed\n", pkg))
  }
}

cat("\n=======================================================\n")
cat("  All packages ready!\n\n")
cat("  Launch the dashboard:\n")
cat("    shiny::runApp('shiny_app')\n")
cat("=======================================================\n")
