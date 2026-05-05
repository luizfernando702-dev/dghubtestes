import { create } from "zustand";
import { persist, createJSONStorage, StateStorage } from "zustand/middleware";
import { get, set, del } from "idb-keyval";
import { createClient } from "@supabase/supabase-js";

// Initialize Supabase client
const SUPABASE_URL = "https://kfwabnwzsubqdvhbhtxd.supabase.co";
const SUPABASE_KEY = "sb_publishable_iawxJFBEnbahGD89j72BEQ_bxrA9iVE";
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Custom storage for idb-keyval
const idbStorage: StateStorage = {
  getItem: async (name: string): Promise<string | null> => {
    return (await get(name)) || null;
  },
  setItem: async (name: string, value: string): Promise<void> => {
    await set(name, value);
  },
  removeItem: async (name: string): Promise<void> => {
    await del(name);
  },
};

export type CacheTable =
  | "tabela_frete_correios"
  | "chegol_regioes"
  | "latam_aeroportos"
  | "configuracoes_servicos"
  | "saoluiz_abrangencia"
  | "carex_abrangencia"
  | "primex_abrangencia"
  | "brix_tarifario"
  | "transportadoras"
  | "produtos"
  | "usuarios";

export const CACHE_TABLES: CacheTable[] = [
  "tabela_frete_correios",
  "chegol_regioes",
  "latam_aeroportos",
  "configuracoes_servicos",
  "saoluiz_abrangencia",
  "carex_abrangencia",
  "primex_abrangencia",
  "brix_tarifario",
  "transportadoras",
  "produtos",
  "usuarios",
];

export interface CacheEntry<T = any> {
  data: T[];
  versao: number;
}

interface CacheState {
  cache: Record<string, CacheEntry>;
  isInitialized: boolean;
  initializeCache: () => Promise<void>;
  updateTableCache: (
    tableName: CacheTable,
    data: any[],
    versao: number,
  ) => void;
  handleRealtimeEvent: (tableName: CacheTable, payload: any) => void;
  getTableData: <T>(tableName: CacheTable) => T[];
}

export const useCacheStore = create<CacheState>()(
  persist(
    (set, get) => ({
      cache: {},
      isInitialized: false,

      initializeCache: async () => {
        if (get().isInitialized) return;
        try {
          // 1. Fetch cache_controle
          const { data: controleData, error: controleError } = await supabase
            .from("cache_controle")
            .select("tabela, versao");

          if (controleError) {
            console.warn(
              "cache_controle table might not exist or is inaccessible. Proceeding without version control.",
              controleError,
            );
          }

          const currentCache = get().cache;
          const tablesToUpdate: CacheTable[] = [];
          const remoteVersions: Record<string, number> = {};

          if (controleData) {
            controleData.forEach((row: any) => {
              remoteVersions[row.tabela] = row.versao;
              const localEntry = currentCache[row.tabela];

              // If local cache doesn't exist or version is different, mark for update
              if (!localEntry || localEntry.versao !== row.versao) {
                if (CACHE_TABLES.includes(row.tabela as CacheTable)) {
                  tablesToUpdate.push(row.tabela as CacheTable);
                }
              }
            });
          }

          // Also check if any CACHE_TABLES are missing from cache_controle
          CACHE_TABLES.forEach((table) => {
            if (!remoteVersions[table] && !currentCache[table]) {
              tablesToUpdate.push(table);
            }
          });

          // 2. Fetch data for tables that need updating
          const fetchPromises = tablesToUpdate.map(async (table) => {
            const { data, error } = await supabase.from(table).select("*");
            if (error) {
              console.error(`Error fetching ${table} for cache:`, error);
              return null;
            }
            return { table, data, versao: remoteVersions[table] || 0 };
          });

          const results = await Promise.all(fetchPromises);

          // 3. Update local state
          set((state) => {
            const newCache = { ...state.cache };
            results.forEach((result) => {
              if (result) {
                newCache[result.table] = {
                  data: result.data,
                  versao: result.versao,
                };
              }
            });
            return { cache: newCache, isInitialized: true };
          });

          // 4. Setup Realtime subscriptions
          const channel = supabase.channel("cache_sync");

          CACHE_TABLES.forEach((table) => {
            channel.on(
              "postgres_changes",
              { event: "*", schema: "public", table: table },
              (payload) => {
                get().handleRealtimeEvent(table, payload);
              },
            );
          });

          // Also listen to cache_controle changes
          channel.on(
            "postgres_changes",
            { event: "*", schema: "public", table: "cache_controle" },
            async (payload) => {
              const { new: newRow } = payload as any;
              if (
                newRow &&
                newRow.tabela &&
                CACHE_TABLES.includes(newRow.tabela)
              ) {
                const localEntry = get().cache[newRow.tabela];
                if (!localEntry || localEntry.versao !== newRow.versao) {
                  const { data, error } = await supabase
                    .from(newRow.tabela)
                    .select("*");
                  if (!error && data) {
                    get().updateTableCache(newRow.tabela, data, newRow.versao);
                  }
                }
              }
            },
          );

          channel.subscribe();
        } catch (error) {
          console.error("Error initializing cache:", error);
        }
      },

      updateTableCache: (tableName, data, versao) => {
        set((state) => ({
          cache: {
            ...state.cache,
            [tableName]: { data, versao },
          },
        }));
      },

      handleRealtimeEvent: (tableName, payload) => {
        set((state) => {
          const currentEntry = state.cache[tableName];
          if (!currentEntry) return state;

          let newData = [...currentEntry.data];
          const { eventType, new: newRecord, old: oldRecord } = payload;

          if (eventType === "INSERT") {
            // Check if it already exists to avoid duplicates
            const exists = newData.some(
              (item: any) => item.id === newRecord.id,
            );
            if (!exists) {
              newData.push(newRecord);
            }
          } else if (eventType === "UPDATE") {
            newData = newData.map((item: any) =>
              item.id === newRecord.id ? newRecord : item,
            );
          } else if (eventType === "DELETE") {
            newData = newData.filter((item: any) => item.id !== oldRecord.id);
          }

          return {
            cache: {
              ...state.cache,
              [tableName]: {
                ...currentEntry,
                data: newData,
              },
            },
          };
        });
      },

      getTableData: <T>(tableName: CacheTable): T[] => {
        const entry = get().cache[tableName];
        return entry ? (entry.data as T[]) : [];
      },
    }),
    {
      name: "dg-hub-cache-storage",
      storage: createJSONStorage(() => idbStorage),
    },
  ),
);
