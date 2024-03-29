// Copyright 2022 OnMetal authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"context"
	goflag "flag"
	"fmt"
	"net"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/onmetal/cephlet/pkg/bcr"

	"github.com/onmetal/cephlet/ori/bucket/server"
	"github.com/onmetal/controller-utils/configutils"
	"github.com/onmetal/onmetal-api/broker/common"
	ori "github.com/onmetal/onmetal-api/ori/apis/bucket/v1alpha1"
)

type Options struct {
	Kubeconfig string
	Address    string

	Namespace                  string
	BucketPoolStorageClassName string

	PathSupportedBucketClasses string
	BucketClassSelector        map[string]string
	BucketEndpoint             string
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Kubeconfig, "kubeconfig", o.Kubeconfig, "Path pointing to a kubeconfig file to use.")
	fs.StringVar(&o.Address, "address", "/var/run/cephlet-bucket.sock", "Address to listen on.")

	fs.StringVar(&o.Namespace, "namespace", o.Namespace, "Target Kubernetes namespace to use.")
	fs.StringVar(&o.BucketPoolStorageClassName, "bucket-pool-storage-class-name", o.BucketPoolStorageClassName, "Name of the target bucket pool storage class.")
	fs.StringVar(&o.BucketEndpoint, "bucket-endpoint", o.BucketEndpoint, "Endpoint at which the buckets are reachable.")

	fs.StringToStringVar(&o.BucketClassSelector, "bucket-class-selector", nil, "Selector for bucket classes to report as available.")
	fs.StringVar(&o.PathSupportedBucketClasses, "supported-bucket-classes", o.PathSupportedBucketClasses, "File containing supported bucket classes.")
}

func (o *Options) MarkFlagsRequired(cmd *cobra.Command) {
	_ = cmd.MarkFlagRequired("bucket-pool-storage-class-name")
	_ = cmd.MarkFlagRequired("bucket-endpoint")
}

func Command() *cobra.Command {
	var (
		zapOpts = zap.Options{Development: true}
		opts    Options
	)

	cmd := &cobra.Command{
		Use: "bucket",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			logger := zap.New(zap.UseFlagOptions(&zapOpts))
			ctrl.SetLogger(logger)
			cmd.SetContext(ctrl.LoggerInto(cmd.Context(), ctrl.Log))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd.Context(), opts)
		},
	}

	goFlags := goflag.NewFlagSet("", 0)
	zapOpts.BindFlags(goFlags)
	cmd.PersistentFlags().AddGoFlagSet(goFlags)

	opts.AddFlags(cmd.Flags())
	opts.MarkFlagsRequired(cmd)

	return cmd
}

func Run(ctx context.Context, opts Options) error {
	log := ctrl.LoggerFrom(ctx)
	setupLog := log.WithName("setup")

	cfg, err := configutils.GetConfig(configutils.Kubeconfig(opts.Kubeconfig))
	if err != nil {
		return err
	}

	supportedClasses, err := bcr.LoadBucketClassesFile(opts.PathSupportedBucketClasses)
	if err != nil {
		return fmt.Errorf("failed to load supported bucket classes: %w", err)
	}

	classRegistry, err := bcr.NewBucketClassRegistry(supportedClasses)
	if err != nil {
		return fmt.Errorf("failed to initialize bucket class registry: %w", err)
	}

	srv, err := server.New(cfg, classRegistry, server.Options{
		Namespace:                  opts.Namespace,
		BucketPoolStorageClassName: opts.BucketPoolStorageClassName,
		BucketClassSelector:        opts.BucketClassSelector,
		BucketEndpoint:             opts.BucketEndpoint,
	})
	if err != nil {
		return fmt.Errorf("error creating server: %w", err)
	}

	log.V(1).Info("Cleaning up any previous socket")
	if err := common.CleanupSocketIfExists(opts.Address); err != nil {
		return fmt.Errorf("error cleaning up socket: %w", err)
	}

	log.V(1).Info("Start listening on unix socket", "Address", opts.Address)
	l, err := net.Listen("unix", opts.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Error(err, "Error closing socket")
		}
	}()

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			log := log.WithName(info.FullMethod)
			ctx = ctrl.LoggerInto(ctx, log)
			log.V(1).Info("Request")
			resp, err = handler(ctx, req)
			if err != nil {
				log.Error(err, "Error handling request")
			}
			return resp, err
		}),
	)
	ori.RegisterBucketRuntimeServer(grpcSrv, srv)

	setupLog.Info("Starting server", "Address", l.Addr().String())
	go func() {
		defer func() {
			setupLog.Info("Shutting down server")
			grpcSrv.Stop()
			setupLog.Info("Shut down server")
		}()
		<-ctx.Done()
	}()
	if err := grpcSrv.Serve(l); err != nil {
		return fmt.Errorf("error serving: %w", err)
	}
	return nil
}
