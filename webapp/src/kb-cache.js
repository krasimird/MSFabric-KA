/**
 * KBCache — IndexedDB wrapper for AI analysis results.
 * Stores: field_lineage, execution_chain, table_summary
 * Keyed by export fingerprint (hash of JSON size + workspace count).
 */
class KBCache {
  constructor(dbName = 'gendwh_kb', version = 1) {
    this.dbName = dbName;
    this.version = version;
    this.db = null;
  }

  async open() {
    if (this.db) return this.db;
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(this.dbName, this.version);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains('field_lineage'))
          db.createObjectStore('field_lineage', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('execution_chain'))
          db.createObjectStore('execution_chain', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('table_summary'))
          db.createObjectStore('table_summary', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('meta'))
          db.createObjectStore('meta', { keyPath: 'key' });
      };
      req.onsuccess = (e) => { this.db = e.target.result; resolve(this.db); };
      req.onerror = (e) => reject(e.target.error);
    });
  }

  /** Compute a fingerprint for the current KB export */
  static fingerprint(KB) {
    const wsCount = KB.workspaces ? KB.workspaces.length : 0;
    const qCount = KB.metadata && KB.metadata.queries ? KB.metadata.queries.length : 0;
    const schemaKeys = KB.schemas ? Object.keys(KB.schemas).length : 0;
    return `fp_${wsCount}_${qCount}_${schemaKeys}`;
  }

  /** Check if analysis is already cached for this fingerprint */
  async isAnalyzed(fingerprint) {
    await this.open();
    return new Promise((resolve) => {
      const tx = this.db.transaction('meta', 'readonly');
      const req = tx.objectStore('meta').get('last_fingerprint');
      req.onsuccess = () => {
        const rec = req.result;
        resolve(rec && rec.value === fingerprint && rec.complete === true);
      };
      req.onerror = () => resolve(false);
    });
  }

  /** Save fingerprint metadata */
  async setFingerprint(fingerprint, complete = false) {
    await this.open();
    const tx = this.db.transaction('meta', 'readwrite');
    tx.objectStore('meta').put({ key: 'last_fingerprint', value: fingerprint, complete, timestamp: Date.now() });
    return new Promise((r, j) => { tx.oncomplete = r; tx.onerror = j; });
  }

  /** Generic put into a store */
  async put(storeName, record) {
    await this.open();
    const tx = this.db.transaction(storeName, 'readwrite');
    tx.objectStore(storeName).put(record);
    return new Promise((r, j) => { tx.oncomplete = r; tx.onerror = j; });
  }

  /** Generic get by id */
  async get(storeName, id) {
    await this.open();
    return new Promise((resolve) => {
      const tx = this.db.transaction(storeName, 'readonly');
      const req = tx.objectStore(storeName).get(id);
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => resolve(null);
    });
  }

  /** Get all records from a store */
  async getAll(storeName) {
    await this.open();
    return new Promise((resolve) => {
      const tx = this.db.transaction(storeName, 'readonly');
      const req = tx.objectStore(storeName).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => resolve([]);
    });
  }

  /** Clear a specific store */
  async clear(storeName) {
    await this.open();
    const tx = this.db.transaction(storeName, 'readwrite');
    tx.objectStore(storeName).clear();
    return new Promise((r, j) => { tx.oncomplete = r; tx.onerror = j; });
  }

  /** Clear all stores (for fresh analysis) */
  async clearAll() {
    await Promise.all([
      this.clear('field_lineage'),
      this.clear('execution_chain'),
      this.clear('table_summary'),
      this.clear('meta')
    ]);
  }

  /** Search field_lineage by keyword (client-side filter) */
  async searchLineage(query) {
    const all = await this.getAll('field_lineage');
    const q = query.toLowerCase();
    return all.filter(r => {
      const blob = `${r.target_table||''} ${r.target_field||''} ${r.source_table||''} ${r.source_column||''} ${r.expression||''} ${r.business_logic||''}`.toLowerCase();
      return blob.includes(q);
    });
  }
}

