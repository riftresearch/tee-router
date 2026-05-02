import ExplorerClient from "@/components/ExplorerClient";

export default async function OrderPage({
  params,
}: {
  params: Promise<{ orderId: string }>;
}) {
  const { orderId } = await params;
  const decodedOrderId = decodeURIComponent(orderId);

  return <ExplorerClient key={decodedOrderId} initialOrderId={decodedOrderId} />;
}
