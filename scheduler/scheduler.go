package scheduler

import (
    "context"
    "fmt"
    "os"

    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/homedir"
    "path/filepath"
    batchv1 "k8s.io/api/batch/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronJobScheduler creates a CronJob in the Kubernetes cluster
func CronJob(namespace, jobName, image, schedule string) error {
	log.Println("In creating Cronjob", homedir.HomeDir()
    kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return err
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return err
    }

    job := &batchv1.CronJob{
        ObjectMeta: metav1.ObjectMeta{
            Name:      jobName,
            Namespace: namespace,
        },
        Spec: batchv1.CronJobSpec{
            Schedule:                   schedule,
            JobTemplate: batchv1.JobTemplateSpec{
                Spec: batchv1.JobSpec{
                    Template: corev1.PodTemplateSpec{
                        Spec: corev1.PodSpec{
                            Containers: []corev1.Container{
                                {
                                    Name:  jobName,
                                    Image: image,
                                },
                            },
                            RestartPolicy: corev1.RestartPolicyOnFailure,
                        },
                    },
                },
            },
        },
    }

    _, err = clientset.BatchV1().CronJobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
    if err != nil {
        return err
    }

    fmt.Printf("CronJob %s created successfully\n", jobName)
    return nil
}


func UpdateCronJob(namespace, jobName, image, schedule string) error {
    kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return err
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return err
    }

    // Get the existing CronJob
    existingCronJob, err := clientset.BatchV1().CronJobs(namespace).Get(context.Background(), jobName, metav1.GetOptions{})
    if err != nil {
        return err
    }

    // Update the schedule and image
    existingCronJob.Spec.Schedule = schedule
    existingCronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image = image

    // Update the CronJob
    _, err = clientset.BatchV1().CronJobs(namespace).Update(context.Background(), existingCronJob, metav1.UpdateOptions{})
    if err != nil {
        return err
    }

    fmt.Printf("CronJob %s updated successfully\n", jobName)
    return nil
}

// DeleteCronJob deletes an existing CronJob from the Kubernetes cluster
func DeleteCronJob(namespace, jobName string) error {
    kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return err
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return err
    }

    // Delete the CronJob
    err = clientset.BatchV1().CronJobs(namespace).Delete(context.Background(), jobName, metav1.DeleteOptions{})
    if err != nil {
        return err
    }

    fmt.Printf("CronJob %s deleted successfully\n", jobName)
    return nil
}

// GetCronJobLogs retrieves the logs of a CronJob from the Kubernetes cluster
func GetCronJobLogs(namespace, jobName string) (string, error) {
    kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return "", err
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return "", err
    }

    // Get the pods owned by the CronJob
    pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
        LabelSelector: fmt.Sprintf("job-name=%s", jobName),
    })
    if err != nil {
        return "", err
    }

    // Ensure there is at least one pod
    if len(pods.Items) == 0 {
        return "", fmt.Errorf("no pods found for CronJob %s", jobName)
    }

    // Get logs from the first pod
    pod := pods.Items[0]
    req := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &metav1.PodLogOptions{})
    stream, err := req.Stream(context.Background())
    if err != nil {
        return "", err
    }
    defer stream.Close()

    var buf bytes.Buffer
    _, err = io.Copy(&buf, stream)
    if err != nil {
        return "", err
    }

    return buf.String(), nil
}