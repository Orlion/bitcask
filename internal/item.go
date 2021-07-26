package internal

type Item struct {
	FileId int   `json:"file_id"`
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}
