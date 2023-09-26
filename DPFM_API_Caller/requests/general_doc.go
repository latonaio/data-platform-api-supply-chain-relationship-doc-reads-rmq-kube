package requests

type GeneralDoc struct {
	SupplyChainRelationshipID int    `json:"SupplyChainRelationshipID"`
	Buyer                     int    `json:"Buyer"`
	Seller                    int    `json:"Seller"`
	DocType                   string `json:"DocType"`
	DocVersionID              int    `json:"DocVersionID"`
	DocID                     string `json:"DocID"`
	FileExtension             string `json:"FileExtension"`
	FileName                  string `json:"FileName"`
	FilePath                  string `json:"FilePath"`
	DocIssuerBusinessPartner  int    `json:"DocIssuerBusinessPartner"`
}
