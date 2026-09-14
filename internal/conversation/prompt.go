package conversation

import "strings"

// BuildPrompt turns a conversation history plus a new message into a single
// prompt string suitable for `hermes chat -q` or `codex exec`. The format is
// plain text so it works with any LLM backend.
func BuildPrompt(history []Message, newMsg string) string {
	var b strings.Builder
	for _, m := range history {
		role := "User"
		if m.Role == "assistant" {
			role = "Assistant"
		}
		b.WriteString(role + ": " + m.Content + "\n")
	}
	b.WriteString("User: " + newMsg + "\n")
	b.WriteString("Assistant: ")
	return b.String()
}
