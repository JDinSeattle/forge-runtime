package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
	"github.com/spf13/cobra"
)

func newCommand() *cobra.Command {
	s := settingsFromEnv()
	root := &cobra.Command{Use: "forge", Short: "Submit and control durable coding-agent runs", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringVar(&s.endpoint, "api-url", s.endpoint, "Control API base URL (FORGE_API_URL)")
	root.PersistentFlags().StringVar(&s.tenant, "tenant", s.tenant, "Tenant ID (FORGE_TENANT)")
	root.PersistentFlags().StringVar(&s.stateDir, "state-dir", s.stateDir, "Private idempotency receipt directory")
	root.PersistentFlags().DurationVar(&s.timeout, "timeout", s.timeout, "Deadline for each ordinary HTTP operation")
	project := &cobra.Command{Use: "project", Short: "Manage registered repository projects"}
	root.AddCommand(project)
	var name, source, profile string
	create := &cobra.Command{Use: "create", Short: "Create a project from a registered source/profile pair", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(name) == "" || source == "" || profile == "" {
			return errors.New("--name, --source and --profile are required")
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.CreateProject(ctx, api.CreateProject{Name: name, SourceId: source, ProfileId: profile})
		if err != nil {
			return fmt.Errorf("project creation result unknown; inspect the server before submitting again: %w", err)
		}
		var result api.Project
		if err = decodeResponse(response, &result, 201); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	create.Flags().StringVar(&name, "name", "", "Project name")
	create.Flags().StringVar(&source, "source", "", "Registered source ID")
	create.Flags().StringVar(&profile, "profile", "", "Registered repository profile ID")
	project.AddCommand(create)
	run := &cobra.Command{Use: "run", Short: "Submit, observe, and control runs"}
	root.AddCommand(run)
	run.AddCommand(submitCommand(&s), watchCommand(&s), messageCommand(&s), approveCommand(&s), downloadCommand(&s))
	for _, kind := range []string{"get", "snapshot", "cancel"} {
		kind := kind
		run.AddCommand(&cobra.Command{Use: kind + " RUN_ID", Args: oneID, Short: map[string]string{"get": "Fetch run state", "snapshot": "Fetch state with an atomic event cursor", "cancel": "Request cancellation"}[kind], RunE: func(cmd *cobra.Command, args []string) error {
			client, err := s.client()
			if err != nil {
				return err
			}
			ctx, cancel := s.context(cmd.Context())
			defer cancel()
			var result api.Run
			switch kind {
			case "get":
				response, err := client.GetRun(ctx, args[0])
				if err != nil {
					return err
				}
				if err = decodeResponse(response, &result, 200); err != nil {
					return err
				}
			case "snapshot":
				response, err := client.GetSnapshot(ctx, args[0])
				if err != nil {
					return err
				}
				if err = decodeResponse(response, &result, 200); err != nil {
					return err
				}
			case "cancel":
				response, err := client.CancelRun(ctx, args[0])
				if err != nil {
					return fmt.Errorf("cancel result unknown; fetch the run state: %w", err)
				}
				if err = decodeResponse(response, &result, 202); err != nil {
					return err
				}
			}
			return printJSON(cmd.OutOrStdout(), result)
		}})
	}
	var version uint64
	resume := &cobra.Command{Use: "resume RUN_ID", Short: "Resume the exact reviewed run version", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		if !cmd.Flags().Changed("version") || version == 0 {
			return errors.New("--version must be the run version you reviewed")
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.ResumeRun(ctx, args[0], api.Resume{ExpectedVersion: version})
		if err != nil {
			return fmt.Errorf("resume result unknown; fetch the run state: %w", err)
		}
		var result api.Run
		if err = decodeResponse(response, &result, 202); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	resume.Flags().Uint64Var(&version, "version", 0, "Expected run version")
	run.AddCommand(resume)
	approval := &cobra.Command{Use: "approval APPROVAL_ID", Short: "Fetch an immutable approval binding for review", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.GetApproval(ctx, args[0])
		if err != nil {
			return err
		}
		var result api.Approval
		if err = decodeResponse(response, &result, 200); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	run.AddCommand(approval)
	var after string
	var limit int
	artifacts := &cobra.Command{Use: "artifacts RUN_ID", Short: "List ready artifacts (cursor pagination)", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		if limit < 1 || limit > 1000 {
			return errors.New("--limit must be 1..1000")
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.ListArtifacts(ctx, args[0], &api.ListArtifactsParams{After: &after, Limit: &limit})
		if err != nil {
			return err
		}
		var result []api.Artifact
		if err = decodeResponse(response, &result, 200); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	artifacts.Flags().StringVar(&after, "after", "", "Last artifact ID from previous page")
	artifacts.Flags().IntVar(&limit, "limit", 100, "Page size")
	run.AddCommand(artifacts)
	return root
}
func oneID(cmd *cobra.Command, args []string) error {
	if len(args) != 1 || !validID(args[0]) {
		return errors.New("expected exactly one valid resource ID")
	}
	return nil
}

func submitCommand(s *settings) *cobra.Command {
	var task, taskFile, base, config, key string
	var rounds, calls uint64
	var cost, seconds int64
	cmd := &cobra.Command{Use: "submit PROJECT_ID", Short: "Submit a run with a persisted idempotency receipt", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		text, err := readText(task, taskFile, 64000)
		if err != nil {
			return err
		}
		if base == "" || config == "" {
			return errors.New("--base and --config are required")
		}
		body := api.Submit{Task: text, BaseCommit: base, ConfigId: config, Budget: api.Budget{}}
		if cmd.Flags().Changed("max-rounds") {
			if rounds < 1 || rounds > 1000 {
				return errors.New("--max-rounds must be 1..1000")
			}
			body.Budget.MaxModelRounds = &rounds
		}
		if cmd.Flags().Changed("max-tools") {
			if calls < 1 || calls > 10000 {
				return errors.New("--max-tools must be 1..10000")
			}
			body.Budget.MaxToolCalls = &calls
		}
		if cmd.Flags().Changed("max-cost-microusd") {
			if cost <= 0 {
				return errors.New("--max-cost-microusd must be positive")
			}
			body.Budget.MaxCostMicrousd = &cost
		}
		if cmd.Flags().Changed("max-runtime-seconds") {
			if seconds < 1 || seconds > 86400 {
				return errors.New("--max-runtime-seconds must be 1..86400")
			}
			body.Budget.MaxRuntimeSeconds = &seconds
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		receipt, path, err := s.receipt("/v1/projects/"+args[0]+"/runs", body, key)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "Idempotency-Key: %s (receipt: %s)\n", receipt.Key, path)
		var result api.SubmitResult
		if len(receipt.Response) > 0 {
			if err = json.Unmarshal(receipt.Response, &result); err != nil {
				return err
			}
			result.Reused = true
			return printJSON(cmd.OutOrStdout(), result)
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.SubmitRun(ctx, args[0], &api.SubmitRunParams{IdempotencyKey: receipt.Key}, body)
		if err == nil {
			err = decodeResponse(response, &result, 202)
		}
		if err != nil {
			return fmt.Errorf("submit was not confirmed; retry the unchanged command with the saved key %s: %w", receipt.Key, err)
		}
		if !validID(result.RunId) {
			return errors.New("submit response has invalid run ID; receipt remains pending")
		}
		if err = completeReceipt(path, &receipt, result); err != nil {
			return fmt.Errorf("run %s accepted but receipt update failed: %w", result.RunId, err)
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	cmd.Flags().StringVar(&task, "task", "", "Task text")
	cmd.Flags().StringVar(&taskFile, "task-file", "", "Read task from file")
	cmd.Flags().StringVar(&base, "base", "", "Registered immutable base commit")
	cmd.Flags().StringVar(&config, "config", "", "Server configuration ID")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "Stable key; use a new explicit key for an intentionally new identical run")
	cmd.Flags().Uint64Var(&rounds, "max-rounds", 0, "Reduce model-round budget")
	cmd.Flags().Uint64Var(&calls, "max-tools", 0, "Reduce tool-call budget")
	cmd.Flags().Int64Var(&cost, "max-cost-microusd", 0, "Reduce cost budget (integer micro-US dollars)")
	cmd.Flags().Int64Var(&seconds, "max-runtime-seconds", 0, "Reduce runtime budget")
	return cmd
}

func messageCommand(s *settings) *cobra.Command {
	var text, file, key string
	cmd := &cobra.Command{Use: "message RUN_ID", Short: "Append guidance once, with a persisted idempotency key", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		bodyText, err := readText(text, file, 16000)
		if err != nil {
			return err
		}
		body := api.AddMessage{Text: bodyText}
		client, err := s.client()
		if err != nil {
			return err
		}
		receipt, path, err := s.receipt("/v1/runs/"+args[0]+"/messages", body, key)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "Idempotency-Key: %s (receipt: %s)\n", receipt.Key, path)
		var result api.MessageResult
		if len(receipt.Response) > 0 {
			if err = json.Unmarshal(receipt.Response, &result); err != nil {
				return err
			}
			result.Reused = true
			return printJSON(cmd.OutOrStdout(), result)
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.AddMessage(ctx, args[0], &api.AddMessageParams{IdempotencyKey: receipt.Key}, body)
		if err == nil {
			err = decodeResponse(response, &result, 202)
		}
		if err != nil {
			return fmt.Errorf("message was not confirmed; reuse saved key %s: %w", receipt.Key, err)
		}
		if err = completeReceipt(path, &receipt, result); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	cmd.Flags().StringVar(&text, "text", "", "Guidance text")
	cmd.Flags().StringVar(&file, "file", "", "Read guidance from file")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "Stable message key")
	return cmd
}

func approveCommand(s *settings) *cobra.Command {
	var file string
	var allow, deny bool
	cmd := &cobra.Command{Use: "approve APPROVAL_ID", Short: "Apply the exact binding saved and reviewed with run approval", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		if file == "" || allow == deny {
			return errors.New("provide --binding-file and exactly one of --allow or --deny")
		}
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		raw, err := readBounded(f, 32<<10)
		f.Close()
		if err != nil {
			return err
		}
		var approval api.Approval
		if err = json.Unmarshal(raw, &approval); err != nil {
			return err
		}
		if approval.Id != args[0] || approval.Binding.EffectId == "" || approval.Binding.ArgsHash == "" || approval.Binding.Version == 0 {
			return errors.New("binding file must contain the matching complete run approval response")
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.DecideApproval(ctx, args[0], api.ApprovalDecisionBody{Binding: approval.Binding, Approve: allow})
		if err != nil {
			return fmt.Errorf("approval result unknown; inspect this approval before trying again: %w", err)
		}
		var result api.Run
		if err = decodeResponse(response, &result, 200); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), result)
	}}
	cmd.Flags().StringVar(&file, "binding-file", "", "Saved JSON output of run approval")
	cmd.Flags().BoolVar(&allow, "allow", false, "Approve the reviewed binding")
	cmd.Flags().BoolVar(&deny, "deny", false, "Deny the reviewed binding")
	return cmd
}

func readText(text, file string, max int64) (string, error) {
	if (text == "") == (file == "") {
		return "", errors.New("provide exactly one text value or file")
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return "", err
		}
		defer f.Close()
		raw, err := readBounded(f, max)
		if err != nil {
			return "", err
		}
		text = string(raw)
	}
	if strings.TrimSpace(text) == "" || int64(len(text)) > max {
		return "", fmt.Errorf("text must contain 1..%d bytes", max)
	}
	return text, nil
}
