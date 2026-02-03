import {getPrismaClient} from "../src/prisma/Primsa";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import * as fs from "node:fs/promises";
import {FileHandle} from "node:fs/promises";

dayjs.extend(utc);
dayjs.extend(timezone);

const batchSize = 2000;

function chunkArray<T>(array: T[]): T[][] {
	const chunks: T[][] = [];
	for (let i = 0; i < array.length; i += batchSize) {
		chunks.push(array.slice(i, i + batchSize));
	}
	return chunks;
}

const prismaClient = await getPrismaClient();

const fromDate = "2025-10-01T00:00:00Z";
const toDate = "2026-01-31T23:59:59Z";

const subTenants = await prismaClient.tenants_SubTenants.findMany({
	select: {tenantId: true, parentTenantId: true},
});

const subTenantsMap = new Map<string, string>();
subTenants.forEach(subTenant => {
	subTenantsMap.set(subTenant.tenantId, subTenant.parentTenantId);
});

const tenants = await prismaClient.tenants.findMany({
	select: {id: true, displayName: true},
});

const tenantsMap = new Map<string, string>();
tenants.forEach(tenant => {
	tenantsMap.set(tenant.id, tenant.displayName);
});

const locations = await prismaClient.location.findMany({
	select: {id: true, tenant: true, name: true},
});

interface locationType {
	id: string;
	name: string;
}
const locationsMap = new Map<string, locationType[]>();
locations.forEach(location => {
	if (!locationsMap.has(location.tenant)) {
		locationsMap.set(location.tenant, []);
	}
	locationsMap
		.get(location.tenant)
		?.push({id: location.id, name: location.name});
});

const platformBillings = await prismaClient.platform_Billing.findMany({
	where: {
		timestamp: {gte: fromDate, lte: toDate},
	},
	select: {
		id: true,
		tenant: true,
		type: true,
		typeId: true,
		timestamp: true,
		units: true,
	},
	orderBy: {tenant: "asc"},
});

const platformBillingSmsIdMap = new Map<string, string[]>();
const platformBillingEmailIdMap = new Map<string, string[]>();
platformBillings.forEach(platformBilling => {
	if (!platformBillingSmsIdMap.has(platformBilling.tenant)) {
		platformBillingSmsIdMap.set(platformBilling.tenant, []);
	}
	if (!platformBillingEmailIdMap.has(platformBilling.tenant)) {
		platformBillingEmailIdMap.set(platformBilling.tenant, []);
	}
	if (platformBilling.type.toUpperCase() === "SMS" && platformBilling.typeId) {
		platformBillingSmsIdMap
			.get(platformBilling.tenant)
			?.push(platformBilling.typeId);
	} else if (
		platformBilling.type.toUpperCase() === "EMAIL" &&
		platformBilling.typeId
	) {
		platformBillingEmailIdMap
			.get(platformBilling.tenant)
			?.push(platformBilling.typeId);
	}
});

interface BillingSummary {
	tenantId: string;
	tenantName: string | undefined;
	parentTenantId: string | undefined;
	parentTenantName: string | undefined;
	locationId: string | undefined;
	locationName: string | undefined;
	sms: number[];
	email: number[];
}
const summaryBillings: Record<string, BillingSummary> = {};
interface customerLocationType {
	customerId: string | undefined;
	locationId: string | undefined;
}
const smsCustomerLocations: Record<string, customerLocationType> = {};
const emailCustomerLocations: Record<string, customerLocationType> = {};
const customerLocations: Record<string, string | undefined> = {};

let count = 0;
for (const platformBilling of platformBillings) {
	if (count % 2000 == 0) {
		console.log(
			`Processing: ${((count / platformBillings.length) * 100).toFixed(2)}%`,
		);
	}
	count++;
	let skip = false;
	let locationId: string | undefined;
	let customerId: string | undefined;
	if (platformBilling.type.toUpperCase() === "SMS") {
		const smsKey = `${platformBilling.tenant}_${platformBilling.typeId}`;
		if (!smsCustomerLocations[smsKey]) {
			const batches = chunkArray(
				platformBillingSmsIdMap.get(platformBilling.tenant) ?? [],
			);
			for (const batch of batches) {
				const smses = await prismaClient.communications_SMS.findMany({
					where: {
						tenant: platformBilling.tenant,
						id: {in: batch},
					},
					select: {
						id: true,
						locationId: true,
						customerId: true,
					},
				});

				smses.forEach(sms => {
					smsCustomerLocations[`${platformBilling.tenant}_${sms.id}`] = {
						customerId: sms.customerId ?? undefined,
						locationId: sms.locationId ?? undefined,
					};
				});
			}
		}
		const sms = smsCustomerLocations[smsKey];
		if (sms?.locationId) {
			locationId = sms?.locationId;
		} else if (sms?.customerId) {
			customerId = sms?.customerId;
		}
	} else if (platformBilling.type.toUpperCase() === "EMAIL") {
		const emailKey = `${platformBilling.tenant}_${platformBilling.typeId}`;
		if (!emailCustomerLocations[emailKey]) {
			const batches = chunkArray(
				platformBillingEmailIdMap.get(platformBilling.tenant) ?? [],
			);
			for (const batch of batches) {
				const emails = await prismaClient.communications_Email.findMany({
					where: {
						tenant: platformBilling.tenant,
						id: {
							in: batch,
						},
					},
					select: {
						id: true,
						locationId: true,
						customerId: true,
					},
				});

				emails.forEach(email => {
					emailCustomerLocations[`${platformBilling.tenant}_${email.id}`] = {
						customerId: email.customerId ?? undefined,
						locationId: email.locationId ?? undefined,
					};
				});
			}
		}
		const email = emailCustomerLocations[emailKey];

		if (email?.locationId) {
			locationId = email?.locationId;
		} else if (email?.customerId) {
			customerId = email?.customerId;
		}
	} else {
		skip = true;
	}

	if (!skip) {
		if (!locationId && customerId) {
			const key = `${platformBilling.tenant}_${customerId}`;
			if (key in customerLocations) {
				locationId = customerLocations[key];
			} else {
				const customers = await prismaClient.customers.findMany({
					where: {tenant: platformBilling.tenant},
					select: {id: true, locationId: true},
				});
				customers.forEach(customer => {
					customerLocations[`${platformBilling.tenant}_${customer.id}`] =
						customer.locationId;
				});
				locationId = customerLocations[key];
			}
		}

		const key = `${platformBilling.tenant}_${locationId && locationId.length ? locationId : "Unknown"}`;

		let summaryBilling = summaryBillings[key];
		if (!summaryBilling) {
			const tenantName = tenantsMap.get(platformBilling.tenant);
			const parentTenantId = subTenantsMap.get(platformBilling.tenant);
			const parentTenantName = parentTenantId && tenantsMap.get(parentTenantId);

			const days = dayjs(toDate).diff(dayjs(fromDate), "day") + 1;

			locationsMap.get(platformBilling.tenant)?.forEach(location => {
				summaryBillings[`${platformBilling.tenant}_${location.id}`] = {
					tenantId: platformBilling.tenant,
					tenantName: tenantName,
					parentTenantId: parentTenantId ?? platformBilling.tenant,
					parentTenantName: parentTenantName ?? tenantName,
					locationId: location?.id,
					locationName: location?.name,
					sms: Array(days).fill(0),
					email: Array(days).fill(0),
				};
			});
			summaryBillings[`${platformBilling.tenant}_Unknown`] = {
				tenantId: platformBilling.tenant,
				tenantName: tenantName,
				parentTenantId: parentTenantId ?? platformBilling.tenant,
				parentTenantName: parentTenantName ?? tenantName,
				locationId: undefined,
				locationName: undefined,
				sms: Array(days).fill(0),
				email: Array(days).fill(0),
			};

			summaryBilling = summaryBillings[key];
			if (!summaryBilling) {
				summaryBilling = summaryBillings[`${platformBilling.tenant}_Unknown`];
				console.log(
					`Could not find summary billing for ${key} - reverting to ${platformBilling.tenant}_Unknown`,
				);
			}
			if (!summaryBilling) {
				throw new Error(`Could not find summary billing for ${key}`);
			}
		}
		const index = dayjs(platformBilling.timestamp).diff(dayjs(fromDate), "day");
		if (platformBilling.type.toUpperCase() === "SMS") {
			summaryBilling.sms[index] += platformBilling.units;
		}
		if (platformBilling.type.toUpperCase() === "EMAIL") {
			summaryBilling.email[index] += platformBilling.units;
		}
	}
}

console.log(`Processing: 100%`);

let fileHandle: FileHandle | undefined;
try {
	const fileHandle = await fs.open("sms_emails_summary.csv", "w");

	await fileHandle.write(
		`"Tenant ID","Tenant Name","Parent Tenant ID","Parent Tenant Name","Location ID","Location Name","Date","Type","Value"\n`,
	);
	await fileHandle.sync();

	count = 0;
	for (const summaryBilling of Object.values(summaryBillings)) {
		if (count % 10 == 0) {
			console.log(
				`Outputting: ${((count / platformBillings.length) * 100).toFixed(2)}%`,
			);
		}
		count++;

		for (let index = 0; index < summaryBilling.sms.length; ++index) {
			const value = summaryBilling.sms[index];
			const date = dayjs(fromDate).add(index, "day").format("DD/MM/YYYY");
			await fileHandle.write(
				`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${summaryBilling.locationId ?? ""}","${summaryBilling.locationName ?? ""}","${date}","SMS",${value}\n`,
			);
			await fileHandle.sync();
		}
	}

	for (const summaryBilling of Object.values(summaryBillings)) {
		for (let index = 0; index < summaryBilling.email.length; ++index) {
			const value = summaryBilling.email[index];
			const date = dayjs(fromDate).add(index, "day").format("DD/MM/YYYY");
			await fileHandle.write(
				`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${summaryBilling.locationId ?? ""}","${summaryBilling.locationName ?? ""}","${date}","EMAIL",${value}\n`,
			);
		}
	}
} finally {
	if (fileHandle) {
		await fileHandle.sync();
		await fileHandle.close();
	}
}

console.log(`Outputting: 100%`);
