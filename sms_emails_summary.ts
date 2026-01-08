import {getPrismaClient} from "../src/prisma/Primsa";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";

dayjs.extend(utc);
dayjs.extend(timezone);

const prismaClient = await getPrismaClient();

const fromDate = "2025-10-01T00:00:00Z";
const toDate = "2025-12-31T23:59:59Z";

const subTenants = await prismaClient.tenants_SubTenants.findMany({
	select: {tenantId: true, parentTenantId: true},
});

const tenants = await prismaClient.tenants.findMany({
	select: {id: true, displayName: true},
});

const platformBillings = await prismaClient.platform_Billing.findMany({
	where: {
		timestamp: {gte: fromDate, lte: toDate},
	},
	select: {tenant: true, type: true, timestamp: true, units: true},
});

interface BillingSummary {
	tenantId: string;
	tenantName: string | undefined;
	parentTenantId: string | undefined;
	parentTenantName: string | undefined;
	sms: number[];
	email: number[];
}
const summaryBillings: Record<string, BillingSummary> = {};

platformBillings.forEach(platformBilling => {
	let summaryBilling = summaryBillings[platformBilling.tenant];
	if (!summaryBilling) {
		const tenant = tenants.find(tenant => tenant.id === platformBilling.tenant);
		const subTenant = subTenants.find(
			subTenant => subTenant.tenantId === platformBilling.tenant,
		);
		const parentTenant =
			subTenant && tenants.find(tenant => tenant.id === subTenant.parentTenantId);

		const days = dayjs(toDate).diff(dayjs(fromDate), "day") + 1;

		summaryBilling = {
			tenantId: platformBilling.tenant,
			tenantName: tenant?.displayName,
			parentTenantId: parentTenant?.id,
			parentTenantName: parentTenant?.displayName,
			sms: Array(days).fill(0),
			email: Array(days).fill(0),
		};
		summaryBillings[platformBilling.tenant] = summaryBilling;
	}
	const index = dayjs(platformBilling.timestamp).diff(dayjs(fromDate), "day");
	if (platformBilling.type.toUpperCase() === "SMS") {
		summaryBilling.sms[index] += platformBilling.units;
	}
	if (platformBilling.type.toUpperCase() === "EMAIL") {
		summaryBilling.email[index] += platformBilling.units;
	}
});

console.log(
	`"Tenant ID","Tenant Name","Parent Tenant ID","Parent Tenant Name","Date","Type","Value"`,
);
Object.values(summaryBillings).forEach(summaryBilling => {
	summaryBilling.sms.forEach((value, index) => {
		const date = dayjs(fromDate).add(index, "day").format("DD/MM/YYYY");
		console.log(
			`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${date}","SMS",${value}`,
		);
	});
});
Object.values(summaryBillings).forEach(summaryBilling => {
	summaryBilling.email.forEach((value, index) => {
		const date = dayjs(fromDate).add(index, "day").format("DD/MM/YYYY");
		console.log(
			`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${date}","EMAIL",${value}`,
		);
	});
});
