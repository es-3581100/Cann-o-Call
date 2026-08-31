package diff

import (
	"fmt"
	"strings"
)

type Line struct {
	Op      string `json:"op"`
	OldLine int    `json:"old_line,omitempty"`
	NewLine int    `json:"new_line,omitempty"`
	Text    string `json:"text"`
}

func splitLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimSuffix(s, "\n")

	if s == "" {
		return []string{}
	}

	return strings.Split(s, "\n")
}

func Lines(oldText, newText string) []Line {
	a := splitLines(oldText)
	b := splitLines(newText)

	n := len(a)
	m := len(b)

	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}

	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				dp[i][j] = dp[i+1][j+1] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i][j] = dp[i+1][j]
			} else {
				dp[i][j] = dp[i][j+1]
			}
		}
	}

	out := []Line{}

	i := 0
	j := 0
	oldLine := 1
	newLine := 1

	for i < n && j < m {
		if a[i] == b[j] {
			out = append(out, Line{
				Op:      "context",
				OldLine: oldLine,
				NewLine: newLine,
				Text:    a[i],
			})
			i++
			j++
			oldLine++
			newLine++
		} else if dp[i+1][j] >= dp[i][j+1] {
			out = append(out, Line{
				Op:      "remove",
				OldLine: oldLine,
				Text:    a[i],
			})
			i++
			oldLine++
		} else {
			out = append(out, Line{
				Op:      "add",
				NewLine: newLine,
				Text:    b[j],
			})
			j++
			newLine++
		}
	}

	for i < n {
		out = append(out, Line{
			Op:      "remove",
			OldLine: oldLine,
			Text:    a[i],
		})
		i++
		oldLine++
	}

	for j < m {
		out = append(out, Line{
			Op:      "add",
			NewLine: newLine,
			Text:    b[j],
		})
		j++
		newLine++
	}

	return out
}

func Unified(oldText, newText, path string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("--- %s\n", path))
	sb.WriteString(fmt.Sprintf("+++ %s\n", path))

	for _, line := range Lines(oldText, newText) {
		switch line.Op {
		case "context":
			sb.WriteString(" " + line.Text + "\n")
		case "remove":
			sb.WriteString("-" + line.Text + "\n")
		case "add":
			sb.WriteString("+" + line.Text + "\n")
		}
	}

	return sb.String()
}
