package workspace

import "time"

const (
	FormatV1         = "flatten-workspace/v1"
	ModeZipStructure = "zip-structure"
)

type Envelope struct {
	Format             string           `yaml:"format" json:"format"`
	Mode               string           `yaml:"mode" json:"mode"`
	Source             Source           `yaml:"source" json:"source"`
	Tree               map[string]any   `yaml:"tree" json:"tree"`
	Manifest           []map[string]any `yaml:"manifest" json:"manifest"`
	QuarantinedEntries []any            `yaml:"quarantined_entries" json:"quarantined_entries"`
}

type Source struct {
	Name               string `yaml:"name" json:"name"`
	SHA256             string `yaml:"sha256" json:"sha256"`
	FileCount          int    `yaml:"file_count" json:"file_count"`
	DirectoryCount     int    `yaml:"directory_count" json:"directory_count"`
	ArchiveMemberCount int    `yaml:"archive_member_count" json:"archive_member_count"`
	UnsafeEntryCount   int    `yaml:"unsafe_entry_count" json:"unsafe_entry_count"`
}

type FileMeta struct {
	Content   string `json:"content"`
	Encoding  string `json:"encoding"`
	Kind      string `json:"kind"`
	Language  string `json:"language"`
	MediaType string `json:"media_type"`
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
}

type File struct {
	Path           string `json:"path"`
	Size           int64  `json:"size"`
	SHA256         string `json:"sha256"`
	DeclaredSHA256 string `json:"declared_sha256,omitempty"`
	Encoding       string `json:"encoding"`
	Kind           string `json:"kind,omitempty"`
	Language       string `json:"language,omitempty"`
	MediaType      string `json:"media_type,omitempty"`
	Verified       bool   `json:"verified"`
	Data           []byte `json:"-"`
}

type QuarantineDecision struct {
	Item      string    `json:"item"`
	Decision  string    `json:"decision"`
	Reason    string    `json:"reason,omitempty"`
	DecidedAt time.Time `json:"decided_at"`
	ReceiptID string    `json:"receipt_id,omitempty"`
}

type RootVerification struct {
	Root            string   `json:"root"`
	Exists          bool     `json:"exists"`
	IsDir           bool     `json:"is_dir"`
	NestedGitAbsent bool     `json:"nested_git_absent"`
	GitPresent      bool     `json:"git_present"`
	ManifestPresent bool     `json:"manifest_present"`
	ReadmePresent   bool     `json:"readme_present"`
	FileChecks      int      `json:"file_checks"`
	MissingCount    int      `json:"missing_count"`
	MissingFiles    []string `json:"missing_files,omitempty"`
	Verified        bool     `json:"verified"`
	Notes           []string `json:"notes,omitempty"`
}

type WorkspaceBinding struct {
	ProjectRoot  string           `json:"project_root"`
	BoundAt      time.Time        `json:"bound_at"`
	Status       string           `json:"status"`
	Verification RootVerification `json:"verification"`
	ReceiptID    string           `json:"receipt_id,omitempty"`
}

type Workspace struct {
	ID                  string               `json:"id"`
	Format              string               `json:"format"`
	Mode                string               `json:"mode"`
	Source              Source               `json:"source"`
	Files               map[string]*File     `json:"-"`
	Tree                map[string]any       `json:"-"`
	Directories         []string             `json:"directories,omitempty"`
	Issues              []string             `json:"issues,omitempty"`
	Quarantined         []string             `json:"quarantined,omitempty"`
	QuarantineDecisions []QuarantineDecision `json:"quarantine_decisions,omitempty"`
	QuarantinedBlobs    []*QuarantinedBlob   `json:"quarantined_blobs,omitempty"`
	Binding             *WorkspaceBinding    `json:"binding,omitempty"`
	CreatedAt           time.Time            `json:"created_at"`
	FileCount           int                  `json:"file_count"`
	DirectoryCount      int                  `json:"directory_count"`
	ManifestChecked     bool                 `json:"manifest_checked"`
}

type Summary struct {
	ID              string    `json:"id"`
	Format          string    `json:"format"`
	Mode            string    `json:"mode"`
	Source          Source    `json:"source"`
	FileCount       int       `json:"file_count"`
	DirectoryCount  int       `json:"directory_count"`
	ManifestChecked bool      `json:"manifest_checked"`
	Issues          []string  `json:"issues,omitempty"`
	Quarantined     []string  `json:"quarantined,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

func (w *Workspace) Summary() Summary {
	return Summary{
		ID:              w.ID,
		Format:          w.Format,
		Mode:            w.Mode,
		Source:          w.Source,
		FileCount:       w.FileCount,
		DirectoryCount:  w.DirectoryCount,
		ManifestChecked: w.ManifestChecked,
		Issues:          w.Issues,
		Quarantined:     w.Quarantined,
		CreatedAt:       w.CreatedAt,
	}
}
