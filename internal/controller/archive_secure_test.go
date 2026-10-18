package controller

import (
	"slices"
	"strconv"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

const secureTestEndpoint = "e:1"

func archiveArgsJob(args ...string) *batchv1.Job {
	return &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: archiveContainer, Args: args}},
	}}}}
}

func argsOfBuilt(insecure bool) []string {
	r := &TaskReconciler{Archive: ArchiveConfig{
		Endpoint: secureTestEndpoint, Bucket: "records", Region: "us-east-1",
		CredentialsSecret: "creds", Insecure: insecure,
	}}
	task := &flowv1alpha1.Task{}
	task.Name = "t"
	task.Namespace = "ns"
	job := r.buildArchiveJob(task, "pvc", "taskflow/t", "flow")
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name == archiveContainer {
			return c.Args
		}
	}
	return nil
}

func contains(args []string, want string) bool {
	return slices.Contains(args, want)
}

// The sending Job always carries --secure in the "=" form, which
// archiveStatusOf splits on.
func TestBuildArchiveJobSecureArg(t *testing.T) {
	if args := argsOfBuilt(false); !contains(args, "--secure=true") {
		t.Errorf("default config: want --secure=true in %v", args)
	}
	if args := argsOfBuilt(true); !contains(args, "--secure=false") {
		t.Errorf("insecure config: want --secure=false in %v", args)
	}
}

func TestArchiveStatusAtRecordsScheme(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		r := &TaskReconciler{Archive: ArchiveConfig{Endpoint: secureTestEndpoint, Bucket: "b", Insecure: insecure}}
		if got := r.archiveStatusAt("p").Insecure; got != insecure {
			t.Errorf("Insecure=%v configured: status says %v", insecure, got)
		}
	}
}

// A Job made before --secure existed sends over https, the sidecar's default.
func TestArchiveStatusOfSecure(t *testing.T) {
	base := []string{"--endpoint=e:1", "--bucket=b", "--manifest-key-prefix=p"}
	cases := map[string]struct {
		extra    []string
		insecure bool
	}{
		"absent": {nil, false},
		"https":  {[]string{"--secure=" + strconv.FormatBool(true)}, false},
		"false":  {[]string{"--secure=false"}, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			st, ok := archiveStatusOf(archiveArgsJob(append(append([]string{}, base...), c.extra...)...))
			if !ok {
				t.Fatal("not read")
			}
			if st.Insecure != c.insecure {
				t.Errorf("Insecure=%v, want %v", st.Insecure, c.insecure)
			}
			if st.Endpoint != secureTestEndpoint {
				t.Errorf("endpoint changed to %q", st.Endpoint)
			}
		})
	}
}
