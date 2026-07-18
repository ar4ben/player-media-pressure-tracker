const COLORS = {
  red: "#dc3545",
  green: "#16836d",
  blue: "#247bad",
  amber: "#d48700",
  violet: "#7656ad",
};
const TONE_SPLIT_COLORS = {
  negative: "#de5360",
  nonNegative: "#4f9f87",
};

const formatInteger = new Intl.NumberFormat("en-US");
const formatCompact = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const formatDate = new Intl.DateTimeFormat("en-GB", {
  day: "2-digit",
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});
const formatMonth = new Intl.DateTimeFormat("en-GB", {
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});

const DATASETS = {
  englishCoverage: [
    series("english_coverage", "English", COLORS.red),
    series("english_high_salience_coverage", "High salience", COLORS.green),
  ],
  localCoverage: [
    series("translated_french_coverage", "French", COLORS.blue),
    series("translated_spanish_coverage", "Spanish", COLORS.amber),
  ],
  englishTone: [
    series("english_avg_tone", "English", COLORS.red),
    series("english_high_salience_avg_tone", "High salience", COLORS.green),
  ],
  localTone: [
    series("translated_french_avg_tone", "French", COLORS.blue),
    series("translated_spanish_avg_tone", "Spanish", COLORS.amber),
  ],
  englishNegative: [
    series("english_negative_share", "English", COLORS.red),
    series("english_high_salience_negative_share", "High salience", COLORS.green),
  ],
  localNegative: [
    series("translated_french_negative_share", "French", COLORS.blue),
    series("translated_spanish_negative_share", "Spanish", COLORS.amber),
  ],
  wikipedia: [
    series("wikipedia_en_views", "English", COLORS.green),
    series("wikipedia_fr_views", "French", COLORS.blue),
    series("wikipedia_es_views", "Spanish", COLORS.amber),
  ],
};

const SPIKE_SIGNALS = {
  gdelt_english_coverage: { label: "Media EN", format: "integer" },
  gdelt_french_coverage: { label: "Media FR", format: "integer" },
  gdelt_spanish_coverage: { label: "Media ES", format: "integer" },
  wikipedia_en_views: { label: "Wiki EN", format: "integer" },
  wikipedia_fr_views: { label: "Wiki FR", format: "integer" },
  wikipedia_es_views: { label: "Wiki ES", format: "integer" },
  google_trends_global_interest: {
    label: "Google global",
    weeklyKey: "google_trends_global_avg_interest",
  },
  google_trends_fr_interest: {
    label: "Google FR",
    weeklyKey: "google_trends_fr_avg_interest",
  },
  google_trends_es_interest: {
    label: "Google ES",
    weeklyKey: "google_trends_es_avg_interest",
  },
};

const SPIKE_LANES = [
  "gdelt_english_coverage",
  "gdelt_french_coverage",
  "gdelt_spanish_coverage",
  "wikipedia_en_views",
  "wikipedia_fr_views",
  "wikipedia_es_views",
  "google_trends_global_interest",
  "google_trends_fr_interest",
  "google_trends_es_interest",
];

const SPIKE_GROUP_COLORS = {
  media: COLORS.red,
  wikipedia: COLORS.green,
  google_trends: COLORS.violet,
};

const verticalHoverLine = {
  id: "verticalHoverLine",
  afterDatasetsDraw(chart) {
    if (!chart.scales.x) return;

    const [activePoint] = chart.getActiveElements();
    if (!activePoint) return;

    const { ctx, chartArea } = chart;
    const x = activePoint.element.x;

    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.lineWidth = 1;
    ctx.strokeStyle = "#6b7280";
    ctx.stroke();
    ctx.restore();
  },
};

const centerText = {
  id: "centerText",
  afterDraw(chart) {
    const text = chart.options.plugins.centerText?.text;
    const label = chart.options.plugins.centerText?.label;
    if (!text) return;

    const { ctx, chartArea } = chart;
    const x = (chartArea.left + chartArea.right) / 2;
    const y = (chartArea.top + chartArea.bottom) / 2;

    ctx.save();
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillStyle = "#111827";
    ctx.font = "700 20px Inter, sans-serif";
    ctx.fillText(text, x, y - 6);

    if (label) {
      ctx.fillStyle = "#6b7280";
      ctx.font = "500 11px Inter, sans-serif";
      ctx.fillText(label, x, y + 14);
    }

    ctx.restore();
  },
};

function series(key, label, color) {
  return { key, label, color };
}

async function loadDashboard() {
  const [weekly, spikes, matches] = await Promise.all([
    fetch("./player_weekly.json").then(checkResponse).then((r) => r.json()),
    fetch("./player_spikes.json").then(checkResponse).then((r) => r.json()),
    fetch("./player_matches.json").then(checkResponse).then((r) => r.json()),
  ]);

  configureChartDefaults();
  const matchesByWeek = Object.groupBy(matches, (match) => weekStart(match.date));
  renderSummary(weekly);
  renderCharts(weekly, matches, matchesByWeek);
  renderSpikeTimeline(spikes, weekly, matchesByWeek);
  renderTopSpikeWeeks(spikes, weekly);
}

function checkResponse(response) {
  if (!response.ok) {
    throw new Error(`Could not load ${response.url}: ${response.status}`);
  }
  return response;
}

function configureChartDefaults() {
  Chart.register(verticalHoverLine, centerText);
  Chart.defaults.color = "#59636e";
  Chart.defaults.font.family =
    'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
}

function renderSummary(weekly) {
  const articles = weekly.reduce((total, row) => {
    return (
      total +
      (row.english_coverage || 0) +
      (row.translated_french_coverage || 0) +
      (row.translated_spanish_coverage || 0)
    );
  }, 0);
  const wikipediaViews = weekly.reduce((total, row) => {
    return (
      total +
      (row.wikipedia_en_views || 0) +
      (row.wikipedia_fr_views || 0) +
      (row.wikipedia_es_views || 0)
    );
  }, 0);
  const goals = weekly.reduce((total, row) => total + (row.football_goals || 0), 0);
  const firstWeek = weekly[0];
  const lastWeek = weekly.at(-1);
  const periodStart = addDays(
    firstWeek.week_start,
    firstWeek.days_in_period === 7 ? 0 : 7 - firstWeek.days_in_period,
  );
  const periodEnd = addDays(lastWeek.week_start, lastWeek.days_in_period - 1);

  setText("#kpi-articles", formatInteger.format(articles));
  setText("#kpi-weeks", formatInteger.format(weekly.length));
  setText("#kpi-wiki-views", formatCompact.format(wikipediaViews));
  setText("#kpi-goals", formatInteger.format(goals));
  setText(
    "#date-range",
    `${displayDate(periodStart)} – ${displayDate(periodEnd)}`,
  );
}

function renderCharts(weekly, matches, matchesByWeek) {
  const labels = weekly.map((row) => row.week_start);

  createLineChart("coverage-english-chart", labels, chartData(weekly, DATASETS.englishCoverage), {
    beginAtZero: true,
    min: 0,
    max: 4000,
    formatValue: (value) => formatInteger.format(value),
  });
  createLineChart("coverage-local-chart", labels, chartData(weekly, DATASETS.localCoverage), {
    beginAtZero: true,
    min: 0,
    max: 2000,
    formatValue: (value) => formatInteger.format(value),
  });
  [DATASETS.englishTone, DATASETS.localTone].forEach((definitions, index) => {
    const canvas = index === 0 ? "tone-english-chart" : "tone-local-chart";
    createLineChart(canvas, labels, chartData(weekly, definitions), {
      emphasizeZero: true,
      min: -5,
      max: 5,
      formatValue: (value) => Number(value).toFixed(2),
    });
  });
  [DATASETS.englishNegative, DATASETS.localNegative].forEach(
    (definitions, index) => {
      const canvas =
        index === 0 ? "negative-english-chart" : "negative-local-chart";
      createLineChart(canvas, labels, chartData(weekly, definitions), {
        beginAtZero: true,
        emphasizeZero: true,
        min: 0,
        max: 1,
        formatAxis: (value) => `${Math.round(value * 100)}%`,
        formatValue: (value) => `${(value * 100).toFixed(1)}%`,
      });
    },
  );
  renderOverallToneSplit(weekly);
  createLineChart("wikipedia-chart", labels, chartData(weekly, DATASETS.wikipedia), {
    beginAtZero: true,
    min: 0,
    max: 2500000,
    formatAxis: (value) => formatCompact.format(value),
    formatValue: (value) => formatInteger.format(value),
  });

  const googleSeries = [
    series("google_trends_global_avg_interest", "Global", COLORS.violet),
    series("google_trends_fr_avg_interest", "France", COLORS.blue),
    series("google_trends_es_avg_interest", "Spain", COLORS.amber),
  ];
  [[googleSeries[0]], googleSeries.slice(1)].forEach((definitions, index) => {
    const canvas = index === 0 ? "google-global-chart" : "google-local-chart";
    createLineChart(canvas, labels, indexedData(weekly, definitions), {
      beginAtZero: true,
      max: 100,
      formatValue: (value) => Number(value).toFixed(1),
    });
  });

  renderPerformanceCharts(weekly, matches, matchesByWeek);
}

function renderPerformanceCharts(weekly, matches, matchesByWeek) {
  const matchLabels = matches.map((match) => match.date);
  const scorelessStreak = calculateScorelessStreak(matches);
  const goalStreak = calculateGoalStreak(matches);

  renderScoringMatchSplit(matches);

  createLineChart(
    "goals-match-weeks-chart",
    weekly.map((row) => row.week_start),
    [
      {
        label: "Goals",
        data: weekly.map((row) =>
          row.football_appearances > 0 ? row.football_goals || 0 : null,
        ),
        borderColor: COLORS.green,
        pointRadius: 2,
      },
    ],
    {
      beginAtZero: true,
      formatValue: (value) => formatInteger.format(value || 0),
      tooltipAfterBody: (items) => {
        return performanceTooltip(items, matchesByWeek);
      },
    },
  );

  createLineChart(
    "scoreless-streak-chart",
    matchLabels,
    [
      {
        label: "Consecutive scoreless appearances",
        data: scorelessStreak,
        borderColor: COLORS.violet,
        pointRadius: 2,
      },
    ],
    {
      beginAtZero: true,
      formatValue: (value) => `${formatInteger.format(value)} matches`,
      tooltipAfterBody: (items) => {
        return matchTooltip(matches[items[0].dataIndex]);
      },
    },
  );

  createLineChart(
    "goal-streak-chart",
    matchLabels,
    [
      {
        label: "Goals in current scoring run",
        data: goalStreak,
        borderColor: COLORS.green,
        pointRadius: 2,
      },
    ],
    {
      beginAtZero: true,
      formatValue: (value) => `${formatInteger.format(value)} goals`,
      tooltipAfterBody: (items) => {
        return matchTooltip(matches[items[0].dataIndex]);
      },
    },
  );

  createLineChart(
    "scoring-appearances-chart",
    matchLabels,
    [
      {
        label: "Scored",
        data: matches.map((match) => (match.goals > 0 ? 1 : null)),
        showLine: false,
        borderColor: COLORS.green,
        backgroundColor: COLORS.green,
        pointBackgroundColor: COLORS.green,
        pointRadius: (context) => (context.raw ? 2 : 0),
      },
    ],
    {
      min: 0,
      max: 2,
      formatAxis: (value) => (value === 1 ? "Scored" : ""),
      formatValue: () => "Scored",
      tooltipAfterBody: (items) => {
        return matchTooltip(matches[items[0].dataIndex]);
      },
    },
  );

  createLineChart(
    "scoreless-appearances-chart",
    matchLabels,
    [
      {
        label: "Played, no goal",
        data: matches.map((match) => (match.goals === 0 ? 1 : null)),
        showLine: false,
        borderColor: "#9ca3af",
        backgroundColor: "#9ca3af",
        pointBackgroundColor: "#9ca3af",
        pointRadius: (context) => (context.raw ? 4 : 0),
      },
    ],
    {
      min: 0,
      max: 2,
      formatAxis: (value) => (value === 1 ? "Played, no goal" : ""),
      formatValue: () => "Played, no goal",
      tooltipAfterBody: (items) => {
        return matchTooltip(matches[items[0].dataIndex]);
      },
    },
  );

  createBarChart(
    "minutes-played-bars-chart",
    weekly.map((row) => row.week_start),
    [
      {
        label: "Minutes",
        data: weekly.map((row) => row.football_minutes || 0),
        backgroundColor: COLORS.blue,
      },
    ],
    {
      beginAtZero: true,
      formatValue: (value) => `${formatInteger.format(value)} min`,
      tooltipAfterBody: (items) => {
        return performanceTooltip(items, matchesByWeek);
      },
    },
  );

  createLineChart(
    "cards-chart",
    matchLabels,
    [
      {
        label: "Yellow cards",
        data: matches.map((match) => match.yellow_cards || null),
        borderColor: COLORS.amber,
        backgroundColor: COLORS.amber,
        pointBackgroundColor: COLORS.amber,
        pointRadius: (context) => (context.raw ? 4 : 0),
      },
      {
        label: "Red cards",
        data: matches.map((match) => match.red_cards || null),
        borderColor: COLORS.red,
        backgroundColor: COLORS.red,
        pointBackgroundColor: COLORS.red,
        pointRadius: (context) => (context.raw ? 4 : 0),
      },
    ],
    {
      beginAtZero: true,
      max: 2,
      formatValue: (value) => formatInteger.format(value || 0),
      tooltipAfterBody: (items) => {
        return matchTooltip(matches[items[0].dataIndex]);
      },
    },
  );
}

function renderScoringMatchSplit(matches) {
  createScoringMatchSplitChart("scoring-match-split-chart", matches);
  createScoringMatchSplitChart(
    "scoring-match-split-45-chart",
    matches.filter((match) => match.minutes >= 45),
  );
  createScoringMatchSplitChart(
    "scoring-match-split-under-45-chart",
    matches.filter((match) => match.minutes < 45),
  );
}

function createScoringMatchSplitChart(canvasId, matches) {
  const scoringMatches = matches.filter((match) => match.goals > 0).length;
  const scorelessMatches = matches.length - scoringMatches;
  const total = matches.length;
  const scoringShare = total ? scoringMatches / total : 0;

  new Chart(document.getElementById(canvasId), {
    type: "doughnut",
    data: {
      labels: ["No goal", "Scored"],
      datasets: [
        {
          data: [scorelessMatches, scoringMatches],
          backgroundColor: ["#d1d5db", COLORS.green],
          hoverBackgroundColor: ["#d1d5db", COLORS.green],
          borderColor: "#ffffff",
          borderWidth: 2,
          hoverOffset: 3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      cutout: "68%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            usePointStyle: true,
          },
        },
        centerText: {
          text: `${(scoringShare * 100).toFixed(1)}%`,
          label: "with goals",
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const value = Number(context.raw || 0);
              const share = total ? value / total : 0;
              return `${context.label}: ${formatInteger.format(value)} matches (${(share * 100).toFixed(1)}%)`;
            },
          },
        },
      },
    },
  });
}

function calculateScorelessStreak(matches) {
  let streak = 0;

  return matches.map((match) => {
    if (match.goals > 0) {
      streak = 0;
      return 0;
    }

    streak += 1;
    return streak;
  });
}

function calculateGoalStreak(matches) {
  let goals = 0;

  return matches.map((match) => {
    if (match.goals === 0) {
      goals = 0;
      return 0;
    }

    goals += match.goals;
    return goals;
  });
}

function renderOverallToneSplit(weekly) {
  [
    {
      canvasId: "tone-split-english-chart",
      coverageKey: "english_coverage",
      negativeShareKey: "english_negative_share",
    },
    {
      canvasId: "tone-split-high-salience-chart",
      coverageKey: "english_high_salience_coverage",
      negativeShareKey: "english_high_salience_negative_share",
    },
    {
      canvasId: "tone-split-french-chart",
      coverageKey: "translated_french_coverage",
      negativeShareKey: "translated_french_negative_share",
    },
    {
      canvasId: "tone-split-spanish-chart",
      coverageKey: "translated_spanish_coverage",
      negativeShareKey: "translated_spanish_negative_share",
    },
  ].forEach((item) => {
    createDoughnutChart(item.canvasId, calculateToneSplit(weekly, item));
  });
}

function calculateToneSplit(weekly, { coverageKey, negativeShareKey }) {
  return weekly.reduce(
    (result, row) => {
      const coverage = Number(row[coverageKey] || 0);
      const negativeShare = Number(row[negativeShareKey]);
      if (!coverage || !Number.isFinite(negativeShare)) return result;

      result.negative += coverage * negativeShare;
      result.nonNegative += coverage * (1 - negativeShare);
      return result;
    },
    { negative: 0, nonNegative: 0 },
  );
}

function createDoughnutChart(canvasId, split) {
  const total = split.negative + split.nonNegative;
  const negativeShare = total ? split.negative / total : 0;

  new Chart(document.getElementById(canvasId), {
    type: "doughnut",
    data: {
      labels: ["Negative", "Non-negative"],
      datasets: [
        {
          data: [split.negative, split.nonNegative],
          backgroundColor: [
            TONE_SPLIT_COLORS.negative,
            TONE_SPLIT_COLORS.nonNegative,
          ],
          hoverBackgroundColor: [
            TONE_SPLIT_COLORS.negative,
            TONE_SPLIT_COLORS.nonNegative,
          ],
          borderColor: "#ffffff",
          borderWidth: 2,
          hoverOffset: 3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      cutout: "68%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            usePointStyle: true,
          },
        },
        centerText: {
          text: `${(negativeShare * 100).toFixed(1)}%`,
          label: "negative",
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const value = Number(context.raw || 0);
              const share = total ? value / total : 0;
              return `${context.label}: ${formatInteger.format(Math.round(value))} articles (${(share * 100).toFixed(1)}%)`;
            },
          },
        },
      },
    },
  });
}

function chartData(rows, definitions) {
  return definitions.map((item) => ({
    label: item.label,
    data: rows.map((row) => row[item.key]),
    borderColor: item.color,
  }));
}

function indexedData(rows, definitions) {
  return definitions.map((item) => {
    const peak = Math.max(...rows.map((row) => row[item.key] || 0));
    return {
      label: item.label,
      data: rows.map((row) => (peak ? ((row[item.key] || 0) / peak) * 100 : null)),
      borderColor: item.color,
    };
  });
}

function createLineChart(canvasId, labels, datasets, settings = {}) {
  const gridColor = (context) => {
    if (settings.emphasizeZero && context.tick.value === 0) return "#68737d";
    return "#e5e7eb";
  };
  const gridWidth = (context) => {
    return settings.emphasizeZero && context.tick.value === 0 ? 1.5 : 1;
  };

  new Chart(document.getElementById(canvasId), {
    type: "line",
    data: {
      labels,
      datasets: datasets.map((dataset) => prepareDataset(dataset, settings)),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          left: 8,
        },
      },
      interaction: {
        mode: "index",
        axis: "xy",
        intersect: false,
      },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: {
            boxWidth: 16,
            boxHeight: 2,
            usePointStyle: false,
          },
        },
        tooltip: {
          callbacks: {
            title: (items) => displayDate(items[0].label),
            label: (context) => {
              const rawValue = context.dataset.rawData[context.dataIndex];
              const value = settings.formatValue
                ? settings.formatValue(rawValue)
                : rawValue;
              return `${context.dataset.label}: ${value}`;
            },
            afterBody: settings.tooltipAfterBody,
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            callback(value) {
              return displayMonth(this.getLabelForValue(value));
            },
          },
        },
        y: {
          beginAtZero: settings.beginAtZero || false,
          min: settings.min,
          max: settings.max,
          grid: {
            color: gridColor,
            lineWidth: gridWidth,
          },
          ticks: {
            callback: settings.formatAxis,
          },
        },
      },
    },
  });
}

function createBarChart(canvasId, labels, datasets, settings = {}) {
  new Chart(document.getElementById(canvasId), {
    type: "bar",
    data: {
      labels,
      datasets: datasets.map((dataset) => ({
        borderRadius: 2,
        maxBarThickness: 14,
        ...dataset,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          left: 8,
        },
      },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            usePointStyle: true,
          },
        },
        tooltip: {
          callbacks: {
            title: (items) => displayDate(items[0].label),
            label: (context) => {
              const value = settings.formatValue
                ? settings.formatValue(context.raw)
                : context.raw;
              return `${context.dataset.label}: ${value}`;
            },
            afterBody: settings.tooltipAfterBody,
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            callback(value) {
              return displayMonth(this.getLabelForValue(value));
            },
          },
        },
        y: {
          beginAtZero: settings.beginAtZero || false,
          min: settings.min,
          max: settings.max,
          ticks: {
            callback: settings.formatAxis,
          },
          grid: { color: "#e5e7eb" },
        },
      },
    },
  });
}

function prepareDataset(dataset, settings) {
  const rawData = [...dataset.data];
  const minimum = settings.min ?? -Infinity;
  const maximum = settings.max ?? Infinity;
  const baseRadius = dataset.pointRadius ?? 0;

  const isOverflow = (index) => {
    const value = rawData[index];
    return Number.isFinite(value) && (value < minimum || value > maximum);
  };

  return {
    borderWidth: 1.7,
    pointHoverRadius: 3,
    tension: 0,
    spanGaps: false,
    ...dataset,
    rawData,
    data: rawData.map((value) => {
      return Number.isFinite(value)
        ? Math.max(minimum, Math.min(maximum, value))
        : value;
    }),
    pointRadius: (context) => {
      if (isOverflow(context.dataIndex)) return 4;
      return typeof baseRadius === "function" ? baseRadius(context) : baseRadius;
    },
    pointStyle: (context) => {
      return isOverflow(context.dataIndex) ? "triangle" : "circle";
    },
    pointRotation: (context) => {
      return rawData[context.dataIndex] < minimum ? 180 : 0;
    },
  };
}

function renderSpikeTimeline(spikes, weekly, matchesByWeek) {
  const labels = weekly.map((row) => row.week_start);
  const laneBySignal = Object.fromEntries(
    SPIKE_LANES.map((signal, index) => [signal, index]),
  );
  const googlePeaks = buildGooglePeaks(weekly);
  const allPoints = spikes
    .filter((row) => row.signal in laneBySignal)
    .map((row) => ({
      x: row.week_start,
      y: laneBySignal[row.signal],
      row,
      displayValue: formatSpikeValue(row, googlePeaks),
    }));
  const datasets = SPIKE_LANES.map((signal) => {
    const signalPoints = allPoints.filter((point) => point.row.signal === signal);
    return {
      label: SPIKE_SIGNALS[signal]?.label || signal,
      data: signalPoints,
      showLine: false,
      pointRadius: 5,
      pointHoverRadius: 7,
      pointHitRadius: 10,
      pointBorderWidth: 1,
      pointBorderColor: "#ffffff",
      backgroundColor:
        signalPoints.length > 0
          ? SPIKE_GROUP_COLORS[signalPoints[0].row.signal_group] || COLORS.violet
          : COLORS.violet,
    };
  }).filter((dataset) => dataset.data.length > 0);

  new Chart(document.getElementById("spike-timeline-chart"), {
    type: "line",
    data: {
      labels,
      datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {
        mode: "nearest",
        axis: "xy",
        intersect: false,
      },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: {
            boxWidth: 8,
            boxHeight: 8,
            usePointStyle: true,
            generateLabels() {
              return [
                {
                  text: "Media coverage",
                  fillStyle: SPIKE_GROUP_COLORS.media,
                  strokeStyle: SPIKE_GROUP_COLORS.media,
                  pointStyle: "circle",
                },
                {
                  text: "Wikipedia views",
                  fillStyle: SPIKE_GROUP_COLORS.wikipedia,
                  strokeStyle: SPIKE_GROUP_COLORS.wikipedia,
                  pointStyle: "circle",
                },
                {
                  text: "Google Trends",
                  fillStyle: SPIKE_GROUP_COLORS.google_trends,
                  strokeStyle: SPIKE_GROUP_COLORS.google_trends,
                  pointStyle: "circle",
                },
              ];
            },
          },
        },
        tooltip: {
          callbacks: {
            title: (items) => {
              try {
                const row = items[0].raw.row;
                return `${displayDate(row.week_start)} – ${displayDate(row.week_end)}`;
              } catch (error) {
                console.error("Spike timeline tooltip title error:", error);
                return "Date error";
              }
            },
            label: (context) => {
              try {
                const point = context.raw;
                return `${spikeSignalLabel(point.row)}: ${point.displayValue}`;
              } catch (error) {
                console.error("Spike timeline tooltip label error:", error);
                return "Label error";
              }
            },
            afterLabel: (context) => {
              try {
                const ratio = Number(context.raw.row.spike_ratio);
                if (!Number.isFinite(ratio)) return "Spike strength unavailable";
                return `${ratio.toFixed(1)} times higher than recent median`;
              } catch (error) {
                console.error("Spike timeline tooltip ratio error:", error);
                return "Ratio error";
              }
            },
            afterBody: (items) => {
              try {
                const week = items[0].raw.row.week_start;
                return ["", "Nearby performance:", ...performanceLines(week, matchesByWeek)];
              } catch (error) {
                console.error("Spike timeline tooltip performance error:", error);
                return [];
              }
            },
          },
        },
      },
      scales: {
        x: {
          type: "category",
          labels,
          grid: { display: false },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            callback(value) {
              return displayMonth(this.getLabelForValue(value));
            },
          },
        },
        y: {
          min: -0.5,
          max: SPIKE_LANES.length - 0.5,
          reverse: true,
          grid: { color: "#e5e7eb" },
          afterBuildTicks(axis) {
            axis.ticks = SPIKE_LANES.map((_, index) => ({ value: index }));
          },
          ticks: {
            autoSkip: false,
            stepSize: 1,
            color: "#374151",
            font: {
              weight: "600",
            },
            callback(value) {
              const signal = SPIKE_LANES[value];
              return signal ? SPIKE_SIGNALS[signal].label : "";
            },
          },
        },
      },
    },
  });
}

function renderTopSpikeWeeks(spikes, weekly) {
  const grouped = Object.entries(Object.groupBy(spikes, (row) => row.week_start));
  const topWeeks = grouped
    .map(([week, rows]) => {
      const signalGroups = new Set(rows.map((row) => row.signal_group));
      const maxRatio = Math.max(...rows.map((row) => row.spike_ratio || 0));
      return {
        week,
        rows,
        groupCount: signalGroups.size,
        signalCount: rows.length,
        maxRatio,
      };
    })
    .sort((left, right) => {
      return (
        right.groupCount - left.groupCount ||
        right.signalCount - left.signalCount ||
        right.maxRatio - left.maxRatio ||
        left.week.localeCompare(right.week)
      );
    })
    .slice(0, 5);
  const googlePeaks = buildGooglePeaks(weekly);
  const root = document.querySelector("#top-spike-weeks");

  topWeeks.forEach((item) => {
    const strongestSignals = [...item.rows]
      .sort((left, right) => {
        return (right.spike_ratio || 0) - (left.spike_ratio || 0);
      })
      .slice(0, 3);
    const card = document.createElement("article");
    card.className = "rounded-lg border border-gray-200 bg-white p-4";
    card.innerHTML = `
      <p class="text-xs font-semibold uppercase text-gray-500">
        ${displayDate(item.week)} – ${displayDate(item.rows[0].week_end)}
      </p>
      <p class="mt-2 text-sm text-gray-600">
        <span class="text-2xl font-bold text-gray-900">${item.signalCount}</span>
        spike signals
      </p>
      <p class="mt-5 text-xs font-semibold uppercase text-gray-500">
        Strongest signals:
      </p>
      <div class="mt-3 space-y-1 text-sm text-gray-700">
        ${strongestSignals
          .map((row) => {
            return `<div>${spikeSignalLabel(row)}: <strong>${formatSpikeValue(row, googlePeaks)}</strong></div>`;
          })
          .join("")}
      </div>
    `;
    root.append(card);
  });
}

function formatSpikeValue(row, googlePeaks) {
  const signal = SPIKE_SIGNALS[row.signal] || {
    label: row.signal.replaceAll("_", " "),
    format: "integer",
  };
  let value = row.signal_value;

  if (signal.weeklyKey) {
    const peak = googlePeaks[signal.weeklyKey];
    value = peak ? (value / peak) * 100 : value;
  }

  const formatted =
    signal.format === "integer"
      ? formatInteger.format(value)
      : Number(value).toFixed(1);

  return formatted;
}

function buildGooglePeaks(weekly) {
  return Object.fromEntries(
    Object.values(SPIKE_SIGNALS)
      .filter((signal) => signal.weeklyKey)
      .map((signal) => [
        signal.weeklyKey,
        Math.max(...weekly.map((row) => row[signal.weeklyKey] || 0)),
      ]),
  );
}

function spikeSignalLabel(row) {
  const signal = SPIKE_SIGNALS[row.signal];
  if (!signal) return row.signal.replaceAll("_", " ");
  return signal.label;
}

function formatPerformance(week, matchesByWeek) {
  return performanceLines(week, matchesByWeek)
    .map((line, index) => {
      const classes = index === 0 ? "text-gray-700" : "mt-1 text-gray-700";
      return `<div class="${classes}">${line}</div>`;
    })
    .join("");
}

function performanceTooltip(items, matchesByWeek) {
  return performanceLines(items[0].label, matchesByWeek);
}

function matchTooltip(match) {
  if (!match) return [];

  const goals = `${match.goals} goal${match.goals === 1 ? "" : "s"}`;
  const minutes = `${match.minutes} min`;
  const cards = [
    match.yellow_cards ? `${match.yellow_cards} yellow` : null,
    match.red_cards ? `${match.red_cards} red` : null,
  ].filter(Boolean);

  return [
    `${match.team1} ${match.score} ${match.team2}`,
    match.competition,
    [goals, minutes, ...cards].join(" · "),
  ];
}

function performanceLines(week, matchesByWeek) {
  const matches = matchesByWeek[week] || [];
  if (!matches.length) return ["No tracked matches"];

  return matches.map((match) => {
    const goals = `${match.goals} goal${match.goals === 1 ? "" : "s"}`;
    return `${match.team1} ${match.score} ${match.team2} · ${goals}`;
  });
}

function weekStart(value) {
  const date = new Date(`${value}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() - date.getUTCDay());
  return date.toISOString().slice(0, 10);
}

function addDays(value, days) {
  const date = new Date(`${value}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function displayDate(value) {
  return formatDate.format(new Date(`${value}T00:00:00Z`));
}

function displayMonth(value) {
  return formatMonth.format(new Date(`${value}T00:00:00Z`));
}

function setText(selector, value) {
  document.querySelector(selector).textContent = value;
}

loadDashboard().catch((error) => {
  console.error(error);
  document.querySelector("#error").classList.remove("hidden");
});
