# ============================================================
# Healthcare Analytics Platform – R Shiny Dashboard
# Real-time medallion-layer pipeline monitoring &
# clinical/financial analytics.
#
# LAUNCH OPTIONS (all work without touching RStudio settings):
#
#  1. Mac Terminal:
#       cd /path/to/healthcare-platform
#       R -e "shiny::runApp('shiny_app')"
#
#  2. Docker:
#       docker compose up shiny-dashboard
#       → open http://localhost:3838/healthcare
#
#  3. RStudio – click "Run App" on this file (any working dir)
#
# Data auto-refreshes every 5 seconds from Gold-layer CSVs.
# ============================================================

suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(plotly)
  library(DT)
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(scales)
  library(lubridate)
})

# ── Bulletproof path detection ───────────────────────────────────────────────
# Priority order:
#  1. HEALTHCARE_DATA_PATH env var  (Docker / CI)
#  2. rstudioapi source file path   (RStudio Run App, any working dir)
#  3. getwd() fallback              (shiny::runApp() sets wd = shiny_app/)

.env_override <- Sys.getenv("HEALTHCARE_DATA_PATH", unset = "")

BASE <- if (nchar(.env_override) > 0) {
  # Docker / terminal env-var override → absolute path provided directly
  .env_override
} else {
  # Auto-detect app.R location so relative paths always resolve correctly
  # regardless of what the working directory is when the app is launched.
  .rstudio_dir <- tryCatch({
    if (requireNamespace("rstudioapi", quietly = TRUE) &&
        rstudioapi::isAvailable()) {
      ctx <- rstudioapi::getSourceEditorContext()
      if (!is.null(ctx) && nchar(ctx$path) > 0)
        dirname(normalizePath(ctx$path))   # RStudio "Run App" path
      else
        NULL
    } else {
      NULL
    }
  }, error = function(e) NULL)

  .app_dir <- if (!is.null(.rstudio_dir)) {
    .rstudio_dir
  } else {
    normalizePath(getwd())   # shiny::runApp() sets wd → shiny_app/
  }

  file.path(.app_dir, "..", "data", "output")
}

PATHS <- list(
  dept_kpis    = file.path(BASE, "gold", "dept_kpis",     "data.csv"),
  financial    = file.path(BASE, "gold", "financial",     "data.csv"),
  readmissions = file.path(BASE, "gold", "readmissions",  "data.csv"),
  crit_labs    = file.path(BASE, "gold", "critical_labs", "data.csv"),
  audit        = file.path(BASE, "pipeline_audit_log.json")
)

message("[healthcare] Using data path: ", BASE)

# ── Colour palettes ──────────────────────────────────────────────────────────
DEPT_COLORS <- c(
  CARDIOLOGY  = "#e63946",
  EMERGENCY   = "#f4a261",
  ICU         = "#e76f51",
  NEUROLOGY   = "#457b9d",
  ONCOLOGY    = "#2a9d8f",
  ORTHOPEDICS = "#264653"
)

RISK_COLORS <- c(
  LOW_RISK    = "#2a9d8f",
  MEDIUM_RISK = "#f4a261",
  HIGH_RISK   = "#e63946"
)

TIER_COLORS <- c(
  LOW       = "#2a9d8f",
  MEDIUM    = "#457b9d",
  HIGH      = "#f4a261",
  VERY_HIGH = "#e63946"
)

STATUS_COLORS <- c(
  NORMAL   = "#d4edda",
  WARNING  = "#fff3cd",
  CRITICAL = "#f8d7da",
  UNKNOWN  = "#e2e3e5"
)

# ── App theme ────────────────────────────────────────────────────────────────
app_theme <- bs_theme(
  version    = 5,
  bootswatch = "flatly",
  primary    = "#2c7bb6"
)

# ── Helper: big-number labels ────────────────────────────────────────────────
big_label <- function(x, prefix = "", suffix = "", digits = 1) {
  if (is.na(x) || !is.finite(x)) return("–")
  if (abs(x) >= 1e6) paste0(prefix, round(x / 1e6, digits), "M", suffix)
  else if (abs(x) >= 1e3) paste0(prefix, round(x / 1e3, digits), "K", suffix)
  else paste0(prefix, round(x, digits), suffix)
}

kpi_span <- function(txt) {
  tags$span(style = "font-size:1.75rem; font-weight:700;", txt)
}

# ════════════════════════════════════════════════════════════════════════════
# UI
# ════════════════════════════════════════════════════════════════════════════

ui <- page_navbar(
  title = tags$span(
    tags$span(style = "font-size:1.3rem; font-weight:700; color:#ffffff;",
              "🏥 Healthcare Analytics")
  ),
  theme = app_theme,
  bg    = "#1a1a2e",
  fillable = FALSE,

  # ── TAB 1 · OVERVIEW ──────────────────────────────────────────────────────
  nav_panel(
    title = "Overview",

    layout_columns(
      fill = FALSE,
      value_box("Total Admissions",    uiOutput("kpi_total_adm"),   theme = "primary",   height = "110px"),
      value_box("Avg Length of Stay",  uiOutput("kpi_avg_los"),     theme = "info",      height = "110px"),
      value_box("Total Revenue",       uiOutput("kpi_revenue"),     theme = "success",   height = "110px"),
      value_box("Readmission Rate",    uiOutput("kpi_readmission"), theme = "warning",   height = "110px")
    ),

    layout_columns(
      col_widths = c(8, 4),
      card(
        card_header("Monthly Admissions by Department"),
        plotlyOutput("chart_monthly_admissions", height = "300px")
      ),
      card(
        card_header("Readmission Status Mix"),
        plotlyOutput("chart_readmission_status", height = "300px")
      )
    ),

    card(
      card_header("Department KPI Summary – Latest Month per Department"),
      DTOutput("table_dept_kpi")
    )
  ),

  # ── TAB 2 · FINANCIAL ─────────────────────────────────────────────────────
  nav_panel(
    title = "Financial",

    layout_columns(
      fill = FALSE,
      value_box("Total Revenue",           uiOutput("fin_total_rev"),   theme = "success",   height = "110px"),
      value_box("Avg Revenue / Admission", uiOutput("fin_avg_rev"),     theme = "info",      height = "110px"),
      value_box("Top Revenue Dept",        uiOutput("fin_top_dept"),    theme = "primary",   height = "110px"),
      value_box("Top Insurer",             uiOutput("fin_top_insurer"), theme = "secondary", height = "110px")
    ),

    layout_columns(
      col_widths = c(6, 6),
      card(
        card_header("Revenue by Insurer"),
        plotlyOutput("chart_insurer_revenue", height = "300px")
      ),
      card(
        card_header("Admissions by Cost Tier"),
        plotlyOutput("chart_cost_tier", height = "300px")
      )
    ),

    card(
      card_header("Quarterly Revenue by Department"),
      plotlyOutput("chart_quarterly_revenue", height = "300px")
    ),

    card(
      card_header("Financial Detail by Department & Insurer"),
      DTOutput("table_financial")
    )
  ),

  # ── TAB 3 · CLINICAL ──────────────────────────────────────────────────────
  nav_panel(
    title = "Clinical",

    layout_columns(
      fill = FALSE,
      value_box("Critical Lab Events",   uiOutput("clin_critical_labs"), theme = "danger",    height = "110px"),
      value_box("High-Risk Diagnoses",   uiOutput("clin_high_risk"),     theme = "warning",   height = "110px"),
      value_box("Departments Tracked",   uiOutput("clin_depts"),         theme = "info",      height = "110px"),
      value_box("Avg Lab Turnaround",    uiOutput("clin_avg_tat"),       theme = "primary",   height = "110px")
    ),

    card(
      card_header("Readmission Risk Bubble Chart – Diagnosis × Age Group"),
      plotlyOutput("chart_readmission_risk", height = "380px")
    ),

    layout_columns(
      col_widths = c(7, 5),

      card(
        card_header("Critical Lab Results"),
        layout_columns(
          col_widths = c(4, 4, 4),
          selectInput("filt_dept",   "Department:", choices = "All", selected = "All"),
          selectInput("filt_test",   "Test:",       choices = "All", selected = "All"),
          selectInput("filt_status", "Lab Status:", choices = "All", selected = "All")
        ),
        DTOutput("table_crit_labs")
      ),

      card(
        card_header("Doctor Alert Summary – Top 20 by Critical Events"),
        DTOutput("table_doctor_alerts")
      )
    )
  ),

  # ── TAB 4 · PIPELINE ──────────────────────────────────────────────────────
  nav_panel(
    title = "Pipeline",

    layout_columns(
      fill = FALSE,
      value_box("Pipeline Steps",    uiOutput("pipe_steps"),    theme = "primary",   height = "110px"),
      value_box("Total Records In",  uiOutput("pipe_rec_in"),   theme = "info",      height = "110px"),
      value_box("Total Records Out", uiOutput("pipe_rec_out"),  theme = "success",   height = "110px"),
      value_box("Last Run",          uiOutput("pipe_last_run"), theme = "secondary", height = "110px")
    ),

    card(
      card_header("Records Flow – Input vs Output per Pipeline Step"),
      plotlyOutput("chart_pipeline_flow", height = "280px")
    ),

    card(
      card_header("Audit Log"),
      uiOutput("pipeline_audit_html")
    )
  ),

  # ── Refresh indicator ──────────────────────────────────────────────────────
  nav_spacer(),
  nav_item(
    tags$small(class = "text-muted me-2", "⟳ Live · 5 s refresh")
  )
)

# ════════════════════════════════════════════════════════════════════════════
# SERVER
# ════════════════════════════════════════════════════════════════════════════

server <- function(input, output, session) {

  # ── Reactive file readers (poll every 5 seconds) ──────────────────────────
  dept_kpis <- reactiveFileReader(
    5000, session, PATHS$dept_kpis,
    read_csv, show_col_types = FALSE
  )

  financial <- reactiveFileReader(
    5000, session, PATHS$financial,
    read_csv, show_col_types = FALSE
  )

  readmission_risk <- reactiveFileReader(
    5000, session, PATHS$readmissions,
    read_csv, show_col_types = FALSE
  )

  crit_labs <- reactiveFileReader(
    5000, session, PATHS$crit_labs,
    read_csv, show_col_types = FALSE
  )

  # JSON audit log: fromJSON returns a data.frame for array-of-objects
  audit_log <- reactiveFileReader(
    5000, session, PATHS$audit,
    fromJSON
  )

  # ── Populate Clinical filter dropdowns ────────────────────────────────────
  observe({
    labs <- crit_labs()
    updateSelectInput(session, "filt_dept",
      choices  = c("All", sort(unique(labs$department))))
    updateSelectInput(session, "filt_test",
      choices  = c("All", sort(unique(labs$test_name))))
    updateSelectInput(session, "filt_status",
      choices  = c("All", sort(unique(labs$lab_status))))
  })

  # ── Filtered critical labs ────────────────────────────────────────────────
  filtered_labs <- reactive({
    df <- crit_labs()
    dept   <- input$filt_dept
    test   <- input$filt_test
    status <- input$filt_status
    if (!is.null(dept)   && dept   != "All") df <- df[df$department == dept,   ]
    if (!is.null(test)   && test   != "All") df <- df[df$test_name  == test,   ]
    if (!is.null(status) && status != "All") df <- df[df$lab_status == status, ]
    df
  })


  # ══════════════════════════════════════════════════════════════════════════
  # TAB 1 · OVERVIEW
  # ══════════════════════════════════════════════════════════════════════════

  output$kpi_total_adm <- renderUI({
    kpi_span(format(sum(dept_kpis()$total_admissions, na.rm = TRUE), big.mark = ","))
  })

  output$kpi_avg_los <- renderUI({
    kpi_span(paste0(round(mean(dept_kpis()$avg_los_days, na.rm = TRUE), 1), " days"))
  })

  output$kpi_revenue <- renderUI({
    kpi_span(big_label(sum(dept_kpis()$total_revenue_usd, na.rm = TRUE), prefix = "$"))
  })

  output$kpi_readmission <- renderUI({
    kpi_span(paste0(round(mean(dept_kpis()$readmission_rate_pct, na.rm = TRUE), 1), "%"))
  })

  # Monthly admissions line chart
  output$chart_monthly_admissions <- renderPlotly({
    df <- dept_kpis() %>%
      group_by(department, year_month) %>%
      summarise(admissions = sum(total_admissions), .groups = "drop")

    plot_ly(df,
      x = ~year_month, y = ~admissions,
      color = ~department, colors = DEPT_COLORS,
      type  = "scatter", mode = "lines+markers",
      line   = list(width = 2),
      marker = list(size  = 5)
    ) %>%
      layout(
        xaxis  = list(title = "", tickangle = -45),
        yaxis  = list(title = "Admissions"),
        legend = list(orientation = "h", y = -0.35),
        margin = list(b = 80),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Readmission status donut
  output$chart_readmission_status <- renderPlotly({
    df <- dept_kpis() %>%
      mutate(readmission_status = coalesce(readmission_status, "UNKNOWN")) %>%
      count(readmission_status)

    colors_vec <- unname(STATUS_COLORS[df$readmission_status])
    colors_vec[is.na(colors_vec)] <- "#adb5bd"

    plot_ly(df,
      labels = ~readmission_status, values = ~n,
      type   = "pie", hole = 0.45,
      marker = list(colors = colors_vec),
      textinfo = "percent+label"
    ) %>%
      layout(
        showlegend = FALSE,
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Department KPI summary table – latest month per dept
  output$table_dept_kpi <- renderDT({
    df <- dept_kpis() %>%
      group_by(department) %>%
      slice_max(order_by = year_month, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(
        Department      = department,
        Month           = year_month,
        Admissions      = total_admissions,
        `Avg LOS (d)`   = avg_los_days,
        `Readm. %`      = readmission_rate_pct,
        `ICU Adm.`      = icu_admissions,
        `Avg Cost ($)`  = avg_cost_usd,
        `Revenue ($)`   = total_revenue_usd,
        Status          = readmission_status
      )

    datatable(df,
      rownames = FALSE,
      options  = list(pageLength = 10, dom = "t", ordering = TRUE),
      class    = "table-hover table-sm"
    ) %>%
      formatRound(c("Avg LOS (d)", "Readm. %"), digits = 1) %>%
      formatCurrency(c("Avg Cost ($)", "Revenue ($)"), currency = "$", digits = 0) %>%
      formatStyle("Status",
        backgroundColor = styleEqual(
          c("NORMAL",   "WARNING",   "CRITICAL",  "UNKNOWN"),
          c("#d4edda",  "#fff3cd",   "#f8d7da",   "#e2e3e5")
        )
      )
  })


  # ══════════════════════════════════════════════════════════════════════════
  # TAB 2 · FINANCIAL
  # ══════════════════════════════════════════════════════════════════════════

  output$fin_total_rev <- renderUI({
    kpi_span(big_label(sum(financial()$total_revenue_usd, na.rm = TRUE), prefix = "$"))
  })

  output$fin_avg_rev <- renderUI({
    avg <- mean(financial()$avg_revenue_per_admission, na.rm = TRUE)
    kpi_span(paste0("$", format(round(avg), big.mark = ",")))
  })

  output$fin_top_dept <- renderUI({
    top <- financial() %>%
      group_by(department) %>%
      summarise(rev = sum(total_revenue_usd, na.rm = TRUE), .groups = "drop") %>%
      slice_max(rev, n = 1, with_ties = FALSE)
    kpi_span(top$department[1])
  })

  output$fin_top_insurer <- renderUI({
    top <- financial() %>%
      group_by(insurer_name) %>%
      summarise(rev = sum(total_revenue_usd, na.rm = TRUE), .groups = "drop") %>%
      slice_max(rev, n = 1, with_ties = FALSE)
    kpi_span(top$insurer_name[1])
  })

  # Revenue by insurer bar chart
  output$chart_insurer_revenue <- renderPlotly({
    df <- financial() %>%
      group_by(insurer_name) %>%
      summarise(revenue_m = sum(total_revenue_usd, na.rm = TRUE) / 1e6, .groups = "drop") %>%
      arrange(revenue_m)

    plot_ly(df,
      x    = ~revenue_m,
      y    = ~reorder(insurer_name, revenue_m),
      type = "bar",
      orientation = "h",
      marker = list(color = "#2c7bb6",
                    line  = list(color = "#1a5b99", width = 1)),
      text = ~paste0("$", round(revenue_m, 1), "M"),
      textposition = "outside"
    ) %>%
      layout(
        xaxis  = list(title = "Revenue ($M)"),
        yaxis  = list(title = ""),
        margin = list(l = 100),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Cost tier donut
  output$chart_cost_tier <- renderPlotly({
    df <- financial() %>%
      group_by(cost_tier) %>%
      summarise(admissions = sum(admissions, na.rm = TRUE), .groups = "drop")

    tier_order <- c("LOW", "MEDIUM", "HIGH", "VERY_HIGH")
    df$cost_tier <- factor(df$cost_tier, levels = tier_order)
    df <- df %>% arrange(cost_tier)

    colors_vec <- unname(TIER_COLORS[as.character(df$cost_tier)])

    plot_ly(df,
      labels   = ~cost_tier,
      values   = ~admissions,
      type     = "pie",
      hole     = 0.45,
      marker   = list(colors = colors_vec),
      textinfo = "percent+label"
    ) %>%
      layout(
        showlegend    = TRUE,
        legend        = list(orientation = "h", y = -0.15),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Quarterly revenue grouped bar
  output$chart_quarterly_revenue <- renderPlotly({
    df <- financial() %>%
      group_by(department, admission_quarter) %>%
      summarise(revenue_m = sum(total_revenue_usd, na.rm = TRUE) / 1e6, .groups = "drop")

    plot_ly(df,
      x      = ~admission_quarter,
      y      = ~revenue_m,
      color  = ~department,
      colors = DEPT_COLORS,
      type   = "bar"
    ) %>%
      layout(
        barmode = "group",
        xaxis   = list(title = "Quarter"),
        yaxis   = list(title = "Revenue ($M)"),
        legend  = list(orientation = "h", y = -0.25),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Financial detail table
  output$table_financial <- renderDT({
    df <- financial() %>%
      group_by(Department = department, Insurer = insurer_name) %>%
      summarise(
        Admissions         = sum(admissions, na.rm = TRUE),
        `Revenue ($)`      = sum(total_revenue_usd, na.rm = TRUE),
        `Avg Rev/Adm ($)`  = round(mean(avg_revenue_per_admission, na.rm = TRUE), 0),
        `Avg Procedures`   = round(mean(avg_procedures, na.rm = TRUE), 1),
        .groups = "drop"
      ) %>%
      arrange(desc(`Revenue ($)`))

    datatable(df,
      rownames = FALSE,
      filter   = "top",
      options  = list(pageLength = 10, scrollX = TRUE),
      class    = "table-hover table-sm"
    ) %>%
      formatCurrency(c("Revenue ($)", "Avg Rev/Adm ($)"), currency = "$", digits = 0)
  })


  # ══════════════════════════════════════════════════════════════════════════
  # TAB 3 · CLINICAL
  # ══════════════════════════════════════════════════════════════════════════

  output$clin_critical_labs <- renderUI({
    kpi_span(format(nrow(crit_labs()), big.mark = ","))
  })

  output$clin_high_risk <- renderUI({
    n <- readmission_risk() %>%
      filter(readmission_risk_tier == "HIGH_RISK") %>%
      nrow()
    kpi_span(n)
  })

  output$clin_depts <- renderUI({
    kpi_span(length(unique(crit_labs()$department)))
  })

  output$clin_avg_tat <- renderUI({
    avg <- round(mean(crit_labs()$tat_minutes, na.rm = TRUE), 0)
    kpi_span(paste0(format(avg, big.mark = ","), " min"))
  })

  # Readmission risk bubble chart
  output$chart_readmission_risk <- renderPlotly({
    df <- readmission_risk() %>%
      mutate(
        hover = paste0(
          "<b>", diagnosis, "</b><br>",
          "Age group: ",       age_group, "<br>",
          "Department: ",      department, "<br>",
          "Readm. rate: ",     round(readmission_rate_pct, 1), "%<br>",
          "Avg cost: $",       format(round(avg_cost), big.mark = ","), "<br>",
          "Total admissions: ", total_admissions
        )
      )

    plot_ly(df,
      x     = ~avg_cost,
      y     = ~readmission_rate_pct,
      size  = ~total_admissions,
      color = ~readmission_risk_tier,
      colors = RISK_COLORS,
      text   = ~hover,
      hoverinfo = "text",
      type   = "scatter",
      mode   = "markers",
      marker = list(opacity = 0.72, sizemode = "area", sizeref = 0.5)
    ) %>%
      layout(
        xaxis  = list(title = "Average Cost ($)"),
        yaxis  = list(title = "Readmission Rate (%)"),
        legend = list(
          title = list(text = "Risk Tier"),
          orientation = "h", y = -0.2
        ),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Critical labs table
  output$table_crit_labs <- renderDT({
    df <- filtered_labs() %>%
      select(
        `Lab ID`    = lab_id,
        Patient     = patient_id,
        Dept        = department,
        Doctor      = attending_doctor,
        Test        = test_name,
        Marker      = marker,
        Value       = result_value,
        Unit        = unit,
        `Ref Low`   = reference_low,
        `Ref High`  = reference_high,
        Interp      = result_interpretation,
        `TAT (min)` = tat_minutes,
        Status      = lab_status
      )

    datatable(df,
      rownames = FALSE,
      options  = list(pageLength = 8, scrollX = TRUE, dom = "tip"),
      class    = "table-hover table-sm"
    ) %>%
      formatStyle("Status",
        backgroundColor = styleEqual(
          c("FINAL",   "PRELIMINARY", "CANCELLED"),
          c("#d4edda", "#fff3cd",     "#f8d7da")
        )
      ) %>%
      formatStyle("Interp",
        color      = styleEqual(c("HIGH", "LOW", "CRITICAL"),
                                c("#721c24", "#004085", "#721c24")),
        fontWeight = "bold"
      )
  })

  # Doctor alert summary table
  output$table_doctor_alerts <- renderDT({
    df <- crit_labs() %>%
      group_by(Doctor = attending_doctor, Department = department) %>%
      summarise(
        `Critical Events` = n(),
        `Distinct Tests`  = n_distinct(test_name),
        `Avg TAT (min)`   = round(mean(tat_minutes, na.rm = TRUE), 0),
        .groups = "drop"
      ) %>%
      arrange(desc(`Critical Events`)) %>%
      slice_head(n = 20)

    datatable(df,
      rownames = FALSE,
      options  = list(pageLength = 10, dom = "t", ordering = TRUE),
      class    = "table-hover table-sm"
    ) %>%
      formatStyle("Critical Events",
        background         = styleColorBar(range(df$`Critical Events`), "#f4a261"),
        backgroundSize     = "100% 90%",
        backgroundRepeat   = "no-repeat",
        backgroundPosition = "center"
      )
  })


  # ══════════════════════════════════════════════════════════════════════════
  # TAB 4 · PIPELINE
  # ══════════════════════════════════════════════════════════════════════════

  output$pipe_steps <- renderUI({
    kpi_span(nrow(audit_log()))
  })

  output$pipe_rec_in <- renderUI({
    kpi_span(format(sum(audit_log()$records_in, na.rm = TRUE), big.mark = ","))
  })

  output$pipe_rec_out <- renderUI({
    kpi_span(format(sum(audit_log()$records_out, na.rm = TRUE), big.mark = ","))
  })

  output$pipe_last_run <- renderUI({
    df  <- audit_log()
    ts  <- df$timestamp[nrow(df)]
    kpi_span(ts)
  })

  # Pipeline records flow bar chart
  output$chart_pipeline_flow <- renderPlotly({
    df <- audit_log()

    plot_ly(df) %>%
      add_bars(
        x = ~step, y = ~records_in,
        name   = "Records In",
        marker = list(color = "#2c7bb6")
      ) %>%
      add_bars(
        x = ~step, y = ~records_out,
        name   = "Records Out",
        marker = list(color = "#2a9d8f")
      ) %>%
      layout(
        barmode = "group",
        xaxis   = list(title = "", tickangle = -30),
        yaxis   = list(title = "Records"),
        legend  = list(orientation = "h", y = -0.35),
        margin  = list(b = 80),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)"
      ) %>%
      config(displayModeBar = FALSE)
  })

  # Pipeline audit log as styled HTML table
  output$pipeline_audit_html <- renderUI({
    df <- audit_log()

    layer_badge_class <- function(layer) {
      switch(layer,
        bronze = "bg-warning text-dark",
        silver = "bg-secondary",
        gold   = "bg-info text-dark",
        "bg-secondary"
      )
    }

    rows <- lapply(seq_len(nrow(df)), function(i) {
      row       <- df[i, ]
      status_cls <- if (row$status == "success") "bg-success" else "bg-danger"
      notes_txt  <- if (!is.na(row$notes) && nchar(trimws(row$notes)) > 0)
                        row$notes else tags$em(class = "text-muted", "–")

      tags$tr(
        tags$td(tags$code(row$step)),
        tags$td(tags$span(
          class = paste("badge", layer_badge_class(row$layer)),
          style = "font-size:.75rem;",
          toupper(row$layer)
        )),
        tags$td(tags$span(
          class = paste("badge", status_cls),
          style = "font-size:.75rem;",
          toupper(row$status)
        )),
        tags$td(class = "text-end", format(row$records_in,  big.mark = ",")),
        tags$td(class = "text-end", format(row$records_out, big.mark = ",")),
        tags$td(class = "text-end",
          tags$span(
            class = if (row$records_in > 0 && row$records_out < row$records_in)
                        "text-warning fw-bold" else "text-success",
            format(row$records_in - row$records_out, big.mark = ",")
          )
        ),
        tags$td(paste0(row$elapsed_sec, "s")),
        tags$td(row$timestamp),
        tags$td(notes_txt)
      )
    })

    tags$div(
      class = "table-responsive",
      tags$table(
        class = "table table-hover table-sm align-middle",
        tags$thead(
          class = "table-dark",
          tags$tr(
            lapply(
              c("Step", "Layer", "Status",
                "Records In", "Records Out", "Dropped",
                "Elapsed", "Timestamp", "Notes"),
              tags$th
            )
          )
        ),
        tags$tbody(rows)
      )
    )
  })

}

shinyApp(ui, server)
