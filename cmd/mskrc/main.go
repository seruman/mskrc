package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/peterbourgon/ff/v3/ffcli"
	"golang.org/x/exp/slices"

	"code.selman.me/mskrc/internal"
	"code.selman.me/mskrc/internal/kafconfig"
	"code.selman.me/mskrc/internal/kafkactlconfig"
	"code.selman.me/mskrc/internal/kclconfig"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := realMain(
		ctx,
		os.Args,
		os.Stdout,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func realMain(
	ctx context.Context,
	args []string,
	out io.Writer,
) error {
	exec := args[0]
	var (
		rootFlags    = flag.NewFlagSet(exec, flag.ExitOnError)
		flagAliases  = newFlagPairs(rootFlags, "alias", "Alias for a cluster in [cluster]:[alias] format, can be specified multiple times")
		flagClusters sliceFlag
	)

	rootFlags.Var(&flagClusters, "cluster", "Clusters to include in output, can be specified multiple times")

	kafcmd := &ffcli.Command{
		Name:      "kaf",
		ShortHelp: "Generate kaf config",
		LongHelp:  "Generate kaf config",
		Exec: func(ctx context.Context, _ []string) error {
			awscfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			svc := kafka.NewFromConfig(awscfg)

			clusters, err := getClusters(ctx, svc, flagAliases)
			if err != nil {
				return fmt.Errorf("unable to get clusters: %w", err)
			}

			if len(flagClusters) != 0 {
				clusters = slicesFilter(clusters, func(c internal.Cluster) bool {
					return slices.Contains(flagClusters, c.Name)
				})
			}

			cfg := kafconfig.New(clusters)
			var buf bytes.Buffer
			b, err := cfg.Marshal()
			if err != nil {
				return fmt.Errorf("marshal: %w", err)
			}
			buf.Write(b)

			fmt.Fprintln(out, buf.String())
			return nil
		},
	}

	kclcmd := &ffcli.Command{
		Name:      "kcl",
		ShortHelp: "Generate kcl config",
		LongHelp:  "Generate kcl config",
		Exec: func(ctx context.Context, _ []string) error {
			awscfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}

			svc := kafka.NewFromConfig(awscfg)

			clusters, err := getClusters(ctx, svc, flagAliases)
			if err != nil {
				return fmt.Errorf("unable to get clusters: %w", err)
			}

			if len(flagClusters) != 0 {
				clusters = slicesFilter(clusters, func(c internal.Cluster) bool {
					return slices.Contains(flagClusters, c.Name)
				})
			}

			cfgs := kclconfig.New(clusters)
			for _, cfg := range cfgs {
				var buf bytes.Buffer
				b, err := cfg.Marshal()
				if err != nil {
					return fmt.Errorf("marshal: %w", err)
				}

				buf.Write(b)

				fmt.Fprintln(out, buf.String())
			}
			return nil
		},
	}

	kafkactlcmd := &ffcli.Command{
		Name:      "kafkactl",
		ShortHelp: "Generate kafkactl config",
		LongHelp:  "Generate kafkactl config",
		Exec: func(ctx context.Context, _ []string) error {
			awscfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}

			svc := kafka.NewFromConfig(awscfg)

			clusters, err := getClusters(ctx, svc, flagAliases)
			if err != nil {
				return fmt.Errorf("unable to get clusters: %w", err)
			}

			if len(flagClusters) != 0 {
				clusters = slicesFilter(clusters, func(c internal.Cluster) bool {
					return slices.Contains(flagClusters, c.Name)
				})
			}

			cfg := kafkactlconfig.New(clusters)
			var buf bytes.Buffer
			b, err := cfg.Marshal()
			if err != nil {
				return fmt.Errorf("marshal: %w", err)
			}

			buf.Write(b)

			fmt.Fprintln(out, buf.String())
			return nil
		},
	}

	rootcmd := &ffcli.Command{
		FlagSet:     rootFlags,
		Subcommands: []*ffcli.Command{kafcmd, kclcmd, kafkactlcmd},
		Exec: func(_ context.Context, args []string) error {
			if len(args) == 0 {
				rootFlags.Usage()
				return flag.ErrHelp
			}

			return nil
		},
	}

	return rootcmd.ParseAndRun(ctx, args[1:])
}

func getClusters(
	ctx context.Context,
	svc *kafka.Client,
	aliases map[string]string,
) ([]internal.Cluster, error) {
	listClustersResult, err := svc.ListClusters(ctx, &kafka.ListClustersInput{})
	if err != nil {
		return nil, fmt.Errorf("unable to list clusters: %w", err)
	}

	var clusters []internal.Cluster
	for _, ci := range listClustersResult.ClusterInfoList {
		bootstrapout, err := svc.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{
			ClusterArn: ci.ClusterArn,
		})
		if err != nil {
			return nil, fmt.Errorf("arn: %v: %w", ci.ClusterArn, err)
		}

		name := *ci.ClusterName
		alias, _ := aliases[*ci.ClusterName]

		clusters = append(clusters, internal.Cluster{
			Name:    name,
			Alias:   alias,
			Brokers: strings.Split(*bootstrapout.BootstrapBrokerString, ","),
			Version: *ci.CurrentBrokerSoftwareInfo.KafkaVersion,
		})
	}

	return clusters, nil
}

func newFlagPairs(fs *flag.FlagSet, name string, usage string) pairFlag {
	flagPairs := make(pairFlag)
	fs.Var(flagPairs, name, usage)
	return flagPairs
}

type sliceFlag []string

func (s sliceFlag) String() string {
	return strings.Join(s, ",")
}

func (s *sliceFlag) Set(value string) error {
	if !slices.Contains(*s, value) {
		*s = append(*s, value)
	}

	return nil
}

type pairFlag map[string]string

func (h pairFlag) String() string {
	var b bytes.Buffer
	for header, value := range h {
		b.WriteString(fmt.Sprintf("%v: %v", header, value))
	}
	return b.String()
}

func (h pairFlag) Set(value string) error {
	kv := strings.SplitN(value, ":", 2)
	if len(kv) != 2 {
		return fmt.Errorf("invalid pair: %s", value)
	}

	h[kv[0]] = kv[1]

	return nil
}

func slicesFilter[T any](s []T, f func(T) bool) []T {
	var r []T
	for _, v := range s {
		if f(v) {
			r = append(r, v)
		}
	}
	return r
}
