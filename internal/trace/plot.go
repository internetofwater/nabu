package trace

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
)

// Read CSV and plot data
func PlotCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV file: %v", err)
	}

	if len(records) < 2 {
		return fmt.Errorf("not enough data to plot")
	}

	var points plotter.XYs

	// Skip header row
	for i, row := range records[1:] {
		if len(row) < 3 {
			continue
		}

		// Parse timestamp
		timestamp, err := time.Parse(time.RFC3339Nano, row[0])
		if err != nil {
			log.Printf("Skipping row %d due to timestamp parsing error: %v", i+1, err)
			continue
		}

		// Parse duration (in microseconds)
		duration, err := time.ParseDuration(row[2])
		if err != nil {
			log.Printf("Skipping row %d due to duration parsing error: %v", i+1, err)
			continue
		}

		points = append(points, plotter.XY{
			X: float64(timestamp.UnixNano()) / 1e9, // Convert nanoseconds to seconds
			Y: float64(duration.Microseconds()),
		})
	}

	// Create plot
	p := plot.New()
	p.Title.Text = "HTTP Trace Timings"
	p.X.Label.Text = "Time (seconds)"
	p.Y.Label.Text = "Duration (µs)"

	err = plotutil.AddLinePoints(p, "Duration", points)
	if err != nil {
		return fmt.Errorf("failed to plot data: %v", err)
	}

	// Save plot as an image
	err = p.Save(8*72, 6*72, "http_trace.png")
	if err != nil {
		return fmt.Errorf("failed to save plot: %v", err)
	}

	fmt.Println("Plot saved as http_trace.png")
	return nil
}
