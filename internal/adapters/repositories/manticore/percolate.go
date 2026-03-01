package manticore

import (
	"context"
	"fmt"
	"log"
)

func (c *ManticoreClient) CreatePercolateTable(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        content text indexed
    ) type='percolate' 
    charset_table = 'non_cont' 
    ignore_chars = 'U+0300..U+036F' 
    min_infix_len='0' 
    index_exact_words='1' 
    morphology='lemmatize_ru, stem_en' 
    min_stemming_len='4'`, TableGeonameDictPQ)

	req := c.client.UtilsAPI.Sql(ctx).Body(sql).RawResponse(true)
	_, _, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create percolate table: %w", err)
	}

	log.Println("Created percolate table")
	return nil
}

func (c *ManticoreClient) BulkInsertPercolate(ctx context.Context, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	exists, err := c.TableExists(ctx, TableGeonameDictPQ)
	if err != nil {
		return err
	}
	if !exists {
		if err := c.CreatePercolateTable(ctx); err != nil {
			return err
		}
	}

	return c.bulkInsert(ctx, TableGeonameDictPQ, docs)
}
