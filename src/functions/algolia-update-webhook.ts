import { DeliveryClient, IContentItem } from "@kontent-ai/delivery-sdk";
import { IWebhookDeliveryResponse, SignatureHelper } from "@kontent-ai/webhook-helper";
import { Handler } from "@netlify/functions";
import createAlgoliaClient, { SearchIndex } from "algoliasearch";

import { customUserAgent } from "../shared/algoliaUserAgent";
import { hasStringProperty, nameOf } from "../shared/utils/typeguards";
import { AlgoliaItem, canConvertToAlgoliaItem, convertToAlgoliaItem } from "./utils/algoliaItem";
import { createEnvVars } from "./utils/createEnvVars";
import { sdkHeaders } from "./utils/sdkHeaders";
import { serializeUncaughtErrorsHandler } from "./utils/serializeUncaughtErrorsHandler";

const { envVars, missingEnvVars } = createEnvVars(["KONTENT_SECRET", "ALGOLIA_API_KEY"] as const);

// Legacy webhook data
//     {
//       "data": {
//         "system": {
//           "id": "xxx",
//           "name": "Circuit boards and electronics post",
//           "codename": "circuit_boards_and_electronics_post",
//           "collection": "sandbox",
//           "workflow": "default",
//           "workflow_step": "published",
//           "language": "en-US",
//           "type": "article",
//           "last_modified": "2024-02-06T07:53:29.4993828Z"
//         }
//       },
//       "message": {
//         "environment_id": "xxx",
//         "object_type": "content_item",
//         "action": "published",
//         "delivery_slot": "published"
//       }
//     }

// New webhook data
// {
//   "notifications": [
//     {
//       "data": {
//         "system": {
//           "id": "xxx",
//           "name": "Circuit boards and electronics post",
//           "codename": "circuit_boards_and_electronics_post",
//           "collection": "sandbox",
//           "workflow": "default",
//           "workflow_step": "published",
//           "language": "en-US",
//           "type": "article",
//           "last_modified": "2024-02-06T07:53:29.4993828Z"
//         }
//       },
//       "message": {
//         "environment_id": "xxx",
//         "object_type": "content_item",
//         "action": "published",
//         "delivery_slot": "published"
//       }
//     }
//   ]
// }

export const handler: Handler = serializeUncaughtErrorsHandler(async (event) => {
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  if (!event.body) {
    return { statusCode: 400, body: "Missing Data" };
  }

  if (!envVars.KONTENT_SECRET || !envVars.ALGOLIA_API_KEY) {
    return {
      statusCode: 500,
      body: `${missingEnvVars.join(", ")} environment variables are missing, please check the documentation`,
    };
  }

  // Consistency check - make sure your netlify environment variable and your webhook secret matches
  const signatureHelper = new SignatureHelper();
  if (
    !event.headers["x-kc-signature"]
    || !signatureHelper.isValidSignatureFromString(event.body, envVars.KONTENT_SECRET, event.headers["x-kontent-ai-signature"])
  ) { 

    return { statusCode: 401, body: "Unauthorized" };
  }

  const webhookData = JSON.parse(event.body);

  // check if the webhook data is the new format or the old format
  const webhookDataArray = webhookData.notifications ?? [webhookData];

  const queryParams = event.queryStringParameters;
  if (!areValidQueryParams(queryParams)) {
    return { statusCode: 400, body: "Missing some query parameters, please check the documentation" };
  }

  const algoliaClient = createAlgoliaClient(queryParams.appId, envVars.ALGOLIA_API_KEY, { userAgent: customUserAgent });
  const index = algoliaClient.initIndex(queryParams.index);

  const asyncWebhookDataArray = webhookDataArray.map(async (individialData: IWebhookDeliveryResponse) => {

    const deliverClient = new DeliveryClient({
      projectId: individialData.message.project_id,
      globalHeaders: () => sdkHeaders,
    });

    const actions = (await Promise.all(individialData.data.items
      .map(async item => {
        const existingAlgoliaItems = await findAgoliaItems(index, item.codename, item.language);

        if (!existingAlgoliaItems.length) {
          const deliverItems = await findDeliverItemWithChildrenByCodename(deliverClient, item.codename, item.language);
          const deliverItem = deliverItems.get(item.codename);

          if (!deliverItem || (deliverItem.system.type !== "article" && deliverItem.system.type !== "product")) {
            return [{
              objectIdsToRemove: [],
              recordsToReindex: [],
            }]
          }

          return [{
            objectIdsToRemove: [],
            recordsToReindex: canConvertToAlgoliaItem(queryParams.slug)(deliverItem)
              ? [convertToAlgoliaItem(deliverItems, queryParams.slug)(deliverItem)]
              : [],
          }];
        }

        return Promise.all(existingAlgoliaItems
          .map(async i => {
            const deliverItems = await findDeliverItemWithChildrenByCodename(deliverClient, i.codename, i.language);
            const deliverItem = deliverItems.get(i.codename);

            if (!deliverItem || (deliverItem.system.type !== "article" && deliverItem.system.type !== "product")) {
              return {
                objectIdsToRemove: [i.objectID],
                recordsToReindex: [],
              };
            }
            return  {
                objectIdsToRemove: [] as string[],
                recordsToReindex: [convertToAlgoliaItem(deliverItems, queryParams.slug)(deliverItem)],
              }
              
          }));
      }))).flat();

      const recordsToReIndex = [
        ...new Map(actions.flatMap(a => a.recordsToReindex.map(i => [i.codename, i] as const))).values(),
      ];
      const objectIdsToRemove = [...new Set(actions.flatMap(a => a.objectIdsToRemove))];

      const reIndexResponse = recordsToReIndex.length ? await index.saveObjects(recordsToReIndex).wait() : undefined;
      const deletedResponse = objectIdsToRemove.length ? await index.deleteObjects(objectIdsToRemove).wait() : undefined;

      return {
        deletedObjectIds: deletedResponse?.objectIDs,
        reIndexedObjectIds: reIndexResponse?.objectIDs,
      };
  })

  const results = await Promise.all(asyncWebhookDataArray);

  const deletedObjectIds = results.flatMap(r => r.deletedObjectIds ?? []);
  const reIndexedObjectIds = results.flatMap(r => r.reIndexedObjectIds ?? []);

  return {
    statusCode: 200,
    body: JSON.stringify({
      deletedObjectIds,
      reIndexedObjectIds,
    }),
    contentType: "application/json",
  };
});

const findAgoliaItems = async (index: SearchIndex, itemCodename: string, languageCodename: string) => {
  try {
    const response = await index.search<AlgoliaItem>("", {
      facetFilters: [`content.codename: ${itemCodename}`, `language: ${languageCodename}`],
    });

    return response.hits;
  } catch {
    return [];
  }
};

const findDeliverItemWithChildrenByCodename = async (
  deliverClient: DeliveryClient,
  codename: string,
  languageCodename: string,
): Promise<ReadonlyMap<string, IContentItem>> => {
  try {
    const response = await deliverClient
      .item(codename)
      .queryConfig({ waitForLoadingNewContent: true })
      .languageParameter(languageCodename)
      .depthParameter(100)
      .toPromise();

    return new Map([response.data.item, ...Object.values(response.data.linkedItems)].map(i => [i.system.codename, i]));
  } catch {
    return new Map();
  }
};

type ExpectedQueryParams = Readonly<{
  slug: string;
  appId: string;
  index: string;
}>;

const areValidQueryParams = (v: Record<string, unknown> | null): v is ExpectedQueryParams =>
  v !== null
  && hasStringProperty(nameOf<ExpectedQueryParams>("slug"), v)
  && hasStringProperty(nameOf<ExpectedQueryParams>("appId"), v)
  && hasStringProperty(nameOf<ExpectedQueryParams>("index"), v);
