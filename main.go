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
	kafconfig "github.com/birdayz/kaf/pkg/config"
	"github.com/peterbourgon/ff/v3/ffcli"
	"gopkg.in/yaml.v3"
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

	rootFlags := flag.NewFlagSet(exec, flag.ExitOnError)
	flagAliases := newFlagAliases(rootFlags, "alias", "alias for a cluster")

	kafcmd := &ffcli.Command{
		Name:      "kaf",
		ShortHelp: "kaf",
		LongHelp:  "Generate kaf config",
		Exec: func(ctx context.Context, _ []string) error {
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			svc := kafka.NewFromConfig(cfg)

			clusters, err := getClusters(ctx, svc, flagAliases)
			if err != nil {
				return fmt.Errorf("unable to get clusters: %w", err)
			}

			kafcfg := newKafConfig(clusters)

			var cfgout bytes.Buffer
			encoder := yaml.NewEncoder(&cfgout)
			if err := encoder.Encode(&kafcfg); err != nil {
				return fmt.Errorf("unable to encode kaf config: %w", err)
			}

			fmt.Fprintln(out, cfgout.String())
			return nil
		},
	}

	rootcmd := &ffcli.Command{
		FlagSet:     rootFlags,
		Subcommands: []*ffcli.Command{kafcmd},
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
) ([]cluster, error) {
	listClustersResult, err := svc.ListClusters(ctx, &kafka.ListClustersInput{})
	if err != nil {
		return nil, fmt.Errorf("unable to list clusters: %w", err)
	}

	var clusters []cluster
	for _, ci := range listClustersResult.ClusterInfoList {
		bootstrapout, err := svc.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{
			ClusterArn: ci.ClusterArn,
		})
		if err != nil {
			return nil, fmt.Errorf("arn: %v: %w", ci.ClusterArn, err)
		}

		name := *ci.ClusterName
		if a, ok := aliases[*ci.ClusterName]; ok {
			name = a
		}

		clusters = append(clusters, cluster{
			name:    name,
			brokers: strings.Split(*bootstrapout.BootstrapBrokerString, ","),
			version: *ci.CurrentBrokerSoftwareInfo.KafkaVersion,
		})
	}

	return clusters, nil
}

type cluster struct {
	name    string
	brokers []string
	version string
}

func newKafConfig(clusters []cluster) *kafconfig.Config {
	var clustercfgs []*kafconfig.Cluster
	for _, c := range clusters {
		clustercfgs = append(clustercfgs, &kafconfig.Cluster{
			Name:    c.name,
			Brokers: c.brokers,
			Version: c.version,
		})
	}

	return &kafconfig.Config{
		Clusters: clustercfgs,
	}
}

func newFlagAliases(fs *flag.FlagSet, name string, usage string) pairFlag {
	flagPairs := make(pairFlag)
	fs.Var(flagPairs, name, usage)
	return flagPairs
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
