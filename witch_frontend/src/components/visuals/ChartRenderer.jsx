import Plot from 'react-plotly.js';

export function ChartRenderer({ plotJson }) {
  if (!plotJson) {
    return null;
  }

  const { data: originalData, layout: originalLayout } = plotJson;

  // Bloomberg terminal color palette
  const terminalColors = [
    '#ff6b00', // Orange (primary)
    '#00c853', // Green
    '#2196f3', // Blue
    '#ffc107', // Yellow
    '#00bcd4', // Cyan
    '#ff1744', // Red
    '#9c27b0', // Purple
    '#4caf50', // Light green
  ];

  // Apply terminal colors to data
  const data = originalData?.map((trace, idx) => ({
    ...trace,
    marker: {
      ...trace.marker,
      color: trace.marker?.color || terminalColors[idx % terminalColors.length],
    },
    line: {
      ...trace.line,
      color: trace.line?.color || terminalColors[idx % terminalColors.length],
      width: trace.line?.width || 2,
    },
  }));

  // Bloomberg terminal style layout
  const layout = {
    ...originalLayout,
    paper_bgcolor: '#0a0a0a',
    plot_bgcolor: '#0a0a0a',
    font: {
      family: 'IBM Plex Mono, Courier New, monospace',
      color: '#e0e0e0',
      size: 11,
    },
    title: {
      ...originalLayout?.title,
      font: {
        family: 'IBM Plex Mono, Courier New, monospace',
        color: '#ff6b00',
        size: 14,
      },
      y: 0.95,
    },
    xaxis: {
      ...originalLayout?.xaxis,
      gridcolor: '#2a2a2a',
      linecolor: '#3a3a3a',
      tickfont: { 
        color: '#808080',
        family: 'IBM Plex Mono, monospace',
        size: 10,
      },
      title: {
        ...originalLayout?.xaxis?.title,
        font: { 
          color: '#808080', 
          size: 11,
          family: 'IBM Plex Mono, monospace',
        },
      },
      zeroline: true,
      zerolinecolor: '#3a3a3a',
      zerolinewidth: 1,
      showgrid: true,
    },
    yaxis: {
      ...originalLayout?.yaxis,
      gridcolor: '#2a2a2a',
      linecolor: '#3a3a3a',
      tickfont: { 
        color: '#808080',
        family: 'IBM Plex Mono, monospace',
        size: 10,
      },
      title: {
        ...originalLayout?.yaxis?.title,
        font: { 
          color: '#808080', 
          size: 11,
          family: 'IBM Plex Mono, monospace',
        },
      },
      zeroline: true,
      zerolinecolor: '#3a3a3a',
      zerolinewidth: 1,
      showgrid: true,
    },
    legend: {
      ...originalLayout?.legend,
      font: { 
        color: '#e0e0e0',
        family: 'IBM Plex Mono, monospace',
        size: 10,
      },
      bgcolor: '#1a1a1a',
      bordercolor: '#2a2a2a',
      borderwidth: 1,
    },
    margin: {
      l: 60,
      r: 30,
      t: 60,
      b: 50,
    },
    autosize: true,
    hoverlabel: {
      bgcolor: '#1a1a1a',
      bordercolor: '#ff6b00',
      font: {
        family: 'IBM Plex Mono, monospace',
        color: '#e0e0e0',
        size: 11,
      },
    },
  };

  const config = {
    displayModeBar: true,
    displaylogo: false,
    responsive: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d', 'sendDataToCloud'],
    modeBarButtonsToAdd: [],
    linkText: '',
    showLink: false,
    plotlyServerURL: '',
  };

  return (
    <div className="w-full h-full min-h-[400px] bg-[#0a0a0a] border border-[#2a2a2a]">
      <Plot
        data={data}
        layout={layout}
        config={config}
        useResizeHandler={true}
        style={{ width: '100%', height: '100%' }}
      />
    </div>
  );
}
