package main

import (
	"context"
	"fmt"
	//"github.com/aws/aws-sdk-go/service/kafka"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	obskafka "github.com/thanos-community/obslytics/pkg/kafka"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-io/thanos/pkg/model"

	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	infactory "github.com/thanos-community/obslytics/pkg/series/factory"
)

func registerKafka(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("kafka", "Export observability series data into kafka messages.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input-config", "YAML for input, series configuration.", true)
	kafkaFlag := extflag.RegisterPathOrContent(cmd, "kafka-config", "YAML for kafka connection configuration.", true)

	matchersStr := cmd.Flag("match", "Metric matcher for metrics to export (e.g up{a=\"1\"}").Required().String()
	timeFmt := time.RFC3339

	var mint, maxt model.TimeOrDurationValue
	cmd.Flag("min-time", fmt.Sprintf("The lower boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&mint)

	cmd.Flag("max-time", fmt.Sprintf("The upper boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&maxt)

	resolution := cmd.Flag("resolution", "Sample resolution (e.g. 30m)").Required().Duration()
	dbgOut := cmd.Flag("debug", "Show additional debug info (such as produced table)").Bool()

	m["kafka"] = func(g *run.Group, logger log.Logger) error {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			matchers, err := parser.ParseMetricSelector(*matchersStr)
			if err != nil {
				return errors.Wrap(err, "parsing provided matchers")
			}

			inputCfg, err := inputFlag.Content()
			if err != nil {
				return err
			}

			kafkaCfg, err := kafkaFlag.Content()
			if err != nil {
				return err
			}

			kafkaProducer, err := obskafka.NewDataframeKafkaProducer(logger, kafkaCfg)
			if err != nil {
				return err
			}

			in, err := infactory.NewSeriesReader(logger, inputCfg)
			if err != nil {
				return err
			}

			ser, err := in.Read(ctx, series.Params{
				Matchers: matchers,
				MinTime:  timestamp.Time(mint.PrometheusTimestamp()),
				MaxTime:  timestamp.Time(maxt.PrometheusTimestamp()),
			})
			if err != nil {
				return err
			}

			df, err := dataframe.FromSeries(ser, *resolution, func(o *dataframe.AggrsOptions) {
				o.Count.Enabled = true
				o.Sum.Enabled = true
				o.Min.Enabled = true
				o.Max.Enabled = true
			})
			if err != nil {
				return errors.Wrap(err, "dataframe creation")
			}

			if *dbgOut {
				dataframe.Print(os.Stdout, df)
			}

			sent, err := kafkaProducer.Send(df)
            if err != nil {
				kafkaProducer.Close()
				return errors.Wrapf(err, "send kafka messages")
			}
			fmt.Println("Sent", sent, " message(s)")
			kafkaProducer.Close()
			return nil
		}, func(error) { cancel() })
		return nil
	}
}

func exportToKafka(ctx context.Context, df dataframe.Dataframe) (err error) {
	fmt.Println("Exporting to kafka")
	return nil
}
